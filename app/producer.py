# Replay historical CSV observations as individual Kafka messages.
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
        # Close all opened CSV files when leaving this block.
        with ExitStack() as stack:
            try:
                # Look for dataset files by name. Replay needs 12 matching files.
                paths = sorted(
                    Path(data_dir).glob("PRSA_Data_*.csv")
                )

                if len(paths) != expected_files:
                    raise FileNotFoundError(
                        f"Expected {expected_files} station files "
                        f"in {data_dir}; found {len(paths)}"
                    )

                # Create one reader per file. Each row uses the CSV headers
                # as field names, so we can look up values like row["station"].
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
                # Retry file discovery or opening before replay starts.
                # Errors while reading rows later are outside this retry block.
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

            # Each CSV must already be in time order. Merge the files without
            # loading all rows into memory, using station names to break ties.
            # *readers passes each CSV reader as a separate input to the merge.
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

    # The Kafka client calls this function with the delivery result.
    def delivery_report(error, message):
        if error is not None:
            errors.append(str(error))

    # Queue one message. Use the station as the key and send the full row
    # as JSON encoded in UTF-8 bytes, which the consumer decodes later.
    producer.produce(
        topic,
        key=row["station"],
        value=json.dumps(row).encode("utf-8"),
        on_delivery=delivery_report,
    )

    # Wait for delivery results. A nonzero result means work is still pending.
    remaining = producer.flush(timeout=65)

    # Stop the replay if this send failed or delivery is still unconfirmed.
    if errors or remaining:
        raise RuntimeError(
            f"Delivery not confirmed: {errors or 'message still pending'}"
        )


def main():
    # Read replay settings from the command line. The limit counts observations,
    # and the interval is the pause in seconds between sends.
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="air-quality-observations")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--limit", type=int, default=24)
    args = parser.parse_args()

    if args.interval < 0 or args.limit < 1:
        parser.error("Interval must be non-negative and limit must be positive.")

    # Idempotence protects against duplicates from the client's own retries.
    # Starting a new replay can still send the same CSV rows again.
    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 60000,
    })

    sent = 0

    for row in ordered_rows(args.data_dir):
        # Count the row once Kafka confirms delivery. This does not mean
        # the consumer has saved the observation to PostgreSQL yet.
        send_observation(producer, args.topic, row)
        sent += 1

        logging.info(
            "Delivered %s: station=%s observed_at=%s",
            sent,
            row["station"],
            observation_time(row).isoformat(),
        )

        # Check the limit before sleeping so there is no pause after reaching it.
        if sent >= args.limit:
            break

        time.sleep(args.interval)

    logging.info("Replay finished. Confirmed deliveries: %s", sent)


if __name__ == "__main__":
    main()