import argparse
import json
import logging
import os
import time
from datetime import datetime
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer, KafkaException


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

MEASUREMENTS = [
    "PM2.5", "PM10", "SO2", "NO2", "CO", "O3",
    "TEMP", "PRES", "DEWP", "RAIN", "wd", "WSPM",
]

INSERT_SQL = """
    INSERT INTO observations (
        station, observed_at,
        pm25, pm10, so2, no2, co, o3,
        temperature, pressure, dew_point,
        rainfall, wind_direction, wind_speed
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (station, observed_at) DO NOTHING
"""


def measurement_value(value, field):
    if value is None or str(value).strip() in ("", "NA"):
        return None

    if field == "wd":
        return str(value).strip()

    number = Decimal(str(value))

    if not number.is_finite():
        raise ValueError(f"Non-finite measurement in {field}")

    return number


def prepare_observation(row):
    if not isinstance(row, dict):
        raise ValueError("Expected a JSON object")

    station = row["station"].strip()
    if not station:
        raise ValueError("Station must not be empty")

    observed_at = datetime(
        int(row["year"]),
        int(row["month"]),
        int(row["day"]),
        int(row["hour"]),
    )

    values = [
        measurement_value(row[field], field)
        for field in MEASUREMENTS
    ]

    return (station, observed_at, *values)


def store_observation(values):
    """Retry connection failures without advancing Kafka progress."""
    for attempt in range(1, 6):
        try:
            with psycopg.connect(
                host=os.getenv("PGHOST", "postgres"),
                port=5432,
                dbname=os.getenv("PGDATABASE", "air_quality"),
                user=os.getenv("PGUSER", "air_quality"),
                password=os.getenv(
                    "PGPASSWORD", "local_dev_password"
                ),
                connect_timeout=5,
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(INSERT_SQL, values)
                    inserted = cursor.rowcount

            # The connection context commits before reaching this line.
            return inserted

        except (psycopg.OperationalError, psycopg.InterfaceError):
            if attempt == 5:
                raise

            logging.warning(
                "Database unavailable; retrying in 2 seconds "
                "(attempt %s/5)",
                attempt,
            )
            time.sleep(2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if args.limit < 0:
        parser.error("Limit must be zero or positive")

    consumer = Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "air-quality-storage",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })

    consumer.subscribe(["air-quality-observations"])
    logging.info("Consumer started; waiting for observations")
    processed = 0

    try:
        while args.limit == 0 or processed < args.limit:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            row = json.loads(message.value().decode("utf-8"))

            # Handle only the exact connectivity-test message we sent.
            if row == {
                "test": True,
                "message": "pipeline connectivity check",
            }:
                logging.warning(
                    "Skipping known connectivity test at offset %s",
                    message.offset(),
                )
                consumer.commit(
                    message=message, asynchronous=False
                )
                continue

            values = prepare_observation(row)
            inserted = store_observation(values)

            consumer.commit(
                message=message, asynchronous=False
            )
            processed += 1

            logging.info(
                "%s: station=%s observed_at=%s",
                "Inserted" if inserted else "Duplicate ignored",
                values[0],
                values[1].isoformat(),
            )

    finally:
        consumer.close()

    logging.info(
        "Consumer finished. Processed observations: %s",
        processed,
    )


if __name__ == "__main__":
    main()