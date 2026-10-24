import argparse
import csv
import heapq
import json
import logging
import time
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def observation_time(row):
    """Build the original observation time from the CSV date fields."""
    return datetime(
        int(row["year"]),
        int(row["month"]),
        int(row["day"]),
        int(row["hour"]),
    )


def ordered_rows(data_dir):
    """Wait for all station files, then merge them by observation time."""
    expected_files = 12

    while True:
        with ExitStack() as stack:
            try:
                paths = sorted(
                    Path(data_dir).glob("PRSA_Data_*.csv")
                )

                if len(paths) != expected_files:
                    raise FileNotFoundError(
                        f"Expected {expected_files} station files "
                        f"in {data_dir}; found {len(paths)}"
                    )

                readers = [
                    csv.DictReader(
                        stack.enter_context(
                            path.open(
                                encoding="utf-8-sig",
                                newline="",
                            )
                        )
                    )
                    for path in paths
                ]

            except OSError as error:
                logging.warning(
                    "Source unavailable: %s. Retrying in 5 seconds.",
                    error,
                )
                time.sleep(5)
                continue

            logging.info(
                "Source ready: %s station files",
                len(paths),
            )

            yield from heapq.merge(
                *readers,
                key=lambda row: (
                    observation_time(row),
                    row["station"],
                ),
            )
            return


def send_observation(producer, topic, row):
    """Wait for Kafka's delivery acknowledgement before returning."""
    errors = []

    def delivery_report(error, message):
        if error is not None:
            errors.append(str(error))

    producer.produce(
        topic,
        key=row["station"],
        value=json.dumps(row).encode("utf-8"),
        on_delivery=delivery_report,
    )

    remaining = producer.flush(timeout=65)

    if errors or remaining:
        raise RuntimeError(
            f"Delivery not confirmed: {errors or 'message still pending'}"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="air-quality-observations")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--limit", type=int, default=24)
    args = parser.parse_args()

    if args.interval < 0 or args.limit < 1:
        parser.error("Interval must be non-negative and limit must be positive.")

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 60000,
    })

    sent = 0

    for row in ordered_rows(args.data_dir):
        send_observation(producer, args.topic, row)
        sent += 1

        logging.info(
            "Delivered %s: station=%s observed_at=%s",
            sent,
            row["station"],
            observation_time(row).isoformat(),
        )

        if sent >= args.limit:
            break

        time.sleep(args.interval)

    logging.info("Replay finished. Confirmed deliveries: %s", sent)


if __name__ == "__main__":
    main()