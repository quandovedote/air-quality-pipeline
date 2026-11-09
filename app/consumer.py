# Read observations from Kafka, validate them, and store them in PostgreSQL.
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

# Field names in the incoming message. Keep this order aligned with
# the measurement columns in INSERT_SQL: PM2.5 goes into pm25, and so on.
MEASUREMENTS = [
    "PM2.5", "PM10", "SO2", "NO2", "CO", "O3",
    "TEMP", "PRES", "DEWP", "RAIN", "wd", "WSPM",
]

# Psycopg fills the %s placeholders with the values passed to cursor.execute().
# If the same station and observation time already exist, keep the stored row.
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
# Check station names and wind directions against known values.
EXPECTED_STATIONS = {
    "Aotizhongxin", "Changping", "Dingling", "Dongsi",
    "Guanyuan", "Gucheng", "Huairou", "Nongzhanguan",
    "Shunyi", "Tiantan", "Wanliu", "Wanshouxigong",
}

WIND_DIRECTIONS = {
    "N", "NNE", "NE", "ENE",
    "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW",
    "W", "WNW", "NW", "NNW",
}

# Temperature and dew point can be negative, so they are not in this set.
NON_NEGATIVE_FIELDS = {
    "PM2.5", "PM10", "SO2", "NO2", "CO", "O3",
    "RAIN", "WSPM",
}

def measurement_value(value, field):
    # Preserve missing readings as None, which Psycopg writes as SQL NULL.
    if value is None or str(value).strip() in ("", "NA"):
        return None

    # Wind direction is text. Check it against the allowed compass directions.
    if field == "wd":
        direction = str(value).strip()

        if direction not in WIND_DIRECTIONS:
            raise ValueError(
                f"Invalid wind direction: {direction}"
            )

        return direction

    # All other measurements are numeric. Reject NaN and infinity.
    number = Decimal(str(value))

    if not number.is_finite():
        raise ValueError(f"Non-finite measurement in {field}")

    # Pollutants, rainfall and wind speed can be zero; pressure must be positive.
    if field in NON_NEGATIVE_FIELDS and number < 0:
        raise ValueError(f"Negative measurement in {field}")

    if field == "PRES" and number <= 0:
        raise ValueError("Pressure must be positive")

    return number

def prepare_observation(row):
    # Check the message structure before looking up fields by name.
    if not isinstance(row, dict):
        raise ValueError("Expected a JSON object")

    station = row["station"]

    if not isinstance(station, str):
        raise ValueError("Station must be text")

    station = station.strip()

    if station not in EXPECTED_STATIONS:
        raise ValueError(f"Unknown station: {station}")

    # Use the original observation time from the dataset, not the ingestion time.
    observed_at = datetime(
        int(row["year"]),
        int(row["month"]),
        int(row["day"]),
        int(row["hour"]),
    )

    # Validate each measurement in the same order as the SQL columns.
    values = [
        measurement_value(row[field], field)
        for field in MEASUREMENTS
    ]

    # Unpack the measurements so each gets its own SQL placeholder.
    return (station, observed_at, *values)


def store_observation(values):
    """Retry connection failures without advancing Kafka progress."""
    for attempt in range(1, 6):
        try:
            # Use environment settings when provided, with local Compose defaults.
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

                    # 1 means a new row was inserted; 0 means a duplicate was skipped.
                    inserted = cursor.rowcount

            # The connection context commits before reaching this line.
            return inserted

        except (psycopg.OperationalError, psycopg.InterfaceError):
            # Retry up to five times in total. If the last attempt fails,
            # raise the error before main() can commit the Kafka offset.
            if attempt == 5:
                raise

            logging.warning(
                "Database unavailable; retrying in 2 seconds "
                "(attempt %s/5)",
                attempt,
            )
            time.sleep(2)


def main():
    # A limit of 0 keeps the consumer listening. A positive limit stops the run.
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if args.limit < 0:
        parser.error("Limit must be zero or positive")

    consumer = Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "air-quality-storage",
        # Start at the oldest available message if there is no usable saved offset.
        "auto.offset.reset": "earliest",
        # Manage offsets explicitly so progress is saved after database handling.
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })

    consumer.subscribe(["air-quality-observations"])
    logging.info("Consumer started; waiting for observations")
    processed = 0

    try:
        while args.limit == 0 or processed < args.limit:
            message = consumer.poll(timeout=1.0)

            # No message arrived this time; go back and check again.
            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            # Decode the message bytes as UTF-8 text, then parse the JSON.
            row = json.loads(message.value().decode("utf-8"))

            # Skip only the exact connectivity test message. Commit its offset
            # without inserting it or counting it toward the observation limit.
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

            # Save Kafka progress only after PostgreSQL has handled the row.
            # If the row is replayed, the SQL insert skips the duplicate.
            consumer.commit(
                message=message, asynchronous=False
            )

            # Both inserts and skipped duplicates count as processed observations.
            processed += 1

            logging.info(
                "%s: station=%s observed_at=%s",
                "Inserted" if inserted else "Duplicate ignored",
                values[0],
                values[1].isoformat(),
            )

    finally:
        # Close the Kafka connection even if an error leaves the loop.
        consumer.close()

    logging.info(
        "Consumer finished. Processed observations: %s",
        processed,
    )


if __name__ == "__main__":
    main()