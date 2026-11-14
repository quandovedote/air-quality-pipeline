# Compare daily SQL reports with results calculated from the original CSV files.
# This script reads PostgreSQL data but does not insert, update or delete it.
# The complete dataset must already be stored in PostgreSQL.
import csv
import os
from collections import Counter
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import psycopg


# Select reporting columns in the same order as the expected Python results.
# The first two columns identify the station and date.
COLUMNS = """
    station, observation_date, received_hours, valid_pm25_hours,
    available_readings_avg_pm25, coverage_percent, coverage_status
"""


def source_summaries():
    # Find the 12 station CSV files mounted inside the container.
    paths = sorted(Path("/data").glob("PRSA_Data_*.csv"))
    if len(paths) != 12:
        raise RuntimeError(f"Expected 12 CSV files; found {len(paths)}")

    # Store daily totals by station and date.
    # A set records each station name once.
    daily = {}
    stations = set()

    for path in paths:
        with path.open(encoding="utf-8-sig", newline="") as source:
            for row in csv.DictReader(source):
                station = row["station"].strip()
                stations.add(station)

                # Build the observation date from the original CSV fields.
                day = date(
                    int(row["year"]), int(row["month"]), int(row["day"])
                )

                # Create starting totals when this station-day is first found.
                # Positions are: received rows, valid PM2.5 readings, PM2.5 sum.
                # For later rows, setdefault returns the existing totals.
                totals = daily.setdefault(
                    (station, day), [0, 0, Decimal("0")]
                )

                # Count every received observation, even when PM2.5 is missing.
                totals[0] += 1
                value = row["PM2.5"].strip()

                # Only available PM2.5 readings contribute to the count and sum.
                # Decimal avoids the usual binary floating-point approximation.
                if value not in ("", "NA"):
                    totals[1] += 1
                    totals[2] += Decimal(value)

    # Calculate expected daily results separately from the SQL reporting code.
    expected = {}
    for key, (received, valid, total) in daily.items():
        # Divide by the number of available readings, not all received rows.
        # Match SQL rounding to three decimal places.
        # No valid readings means no average: None corresponds to SQL NULL.
        average = (
            (total / valid).quantize(
                Decimal("0.001"), rounding=ROUND_HALF_UP
            )
            if valid else None
        )

        # Compare valid readings with 24 expected hours.
        # Match SQL rounding to one decimal place.
        coverage = (Decimal(valid) * 100 / 24).quantize(
            Decimal("0.1"), rounding=ROUND_HALF_UP
        )

        # Distinguish complete coverage from partial or entirely missing readings.
        # Each entry here has received rows because it came from the CSV files.
        status = (
            "complete" if valid == 24
            else "no_valid_measurements" if valid == 0
            else "incomplete"
        )

        # Keep the values in the same order as the SQL reporting columns.
        expected[key] = (received, valid, average, coverage, status)

    # The original dataset should contain 12 stations and 17,532 station-days.
    # One station-day means one station on one date.
    if len(stations) != 12 or len(expected) != 17532:
        raise RuntimeError("Unexpected source station or station-day count")

    return stations, expected


def verify(label, rows, expected):
    # Use station and date as the key; store the remaining reporting values.
    # Matching by key means SQL rows do not need to arrive in a particular order.
    actual = {tuple(row[:2]): tuple(row[2:]) for row in rows}

    # A dictionary keeps only one value per key.
    # Fewer dictionary entries than SQL rows would indicate duplicate keys.
    if len(rows) != len(actual):
        raise RuntimeError(f"{label}: duplicate station-days")

    # Check every key found in either the CSV calculations or SQL results.
    # Detect missing rows, additional rows and differences in reporting values.
    differences = [
        key for key in actual.keys() | expected.keys()
        if key not in actual or key not in expected
        or actual[key] != expected[key]
    ]

    if differences:
        # Stop the check and show one example to help investigate the problem.
        key = sorted(differences)[0]
        raise RuntimeError(
            f"{label}: {len(differences)} mismatches. "
            f"Example {key}: CSV={expected.get(key)}, "
            f"SQL={actual.get(key)}"
        )

    # Reached only when all station-days and reporting values match.
    print(
        f"PASS: {label}: {len(actual)} station-days, zero mismatches",
        flush=True,
    )

    # The last value in each result is its coverage status.
    # Counter shows how many station-days belong to each status.
    print(
        "Statuses:",
        dict(Counter(value[-1] for value in actual.values())),
        flush=True,
    )


def main():
    # Calculate the expected results directly from the source files first.
    stations, expected = source_summaries()

    # Use environment settings when provided, with local prototype defaults.
    # The hostname "postgres" works inside the Docker Compose network.
    with psycopg.connect(
        host=os.getenv("PGHOST", "postgres"),
        dbname=os.getenv("PGDATABASE", "air_quality"),
        user=os.getenv("PGUSER", "air_quality"),
        password=os.getenv("PGPASSWORD", "local_dev_password"),
        connect_timeout=10,
    ) as connection:
        # Prevent this transaction from changing database data.
        connection.execute("SET TRANSACTION READ ONLY")

        # Retrieve every daily result from the view.
        # fetchall returns the query results as a list of rows.
        rows = connection.execute(
            f"SELECT {COLUMNS} FROM daily_pm25_summary"
        ).fetchall()

        # Compare the view with the independently calculated CSV results.
        verify("Daily summary view", rows, expected)

        # Copy the expected results, leaving the original dictionary unchanged.
        # Add one date before and one after the dataset for each station.
        # These dates should have zero counts, zero coverage and no average.
        extended = dict(expected)
        for station in stations:
            for day in (date(2013, 2, 28), date(2017, 3, 1)):
                extended[(station, day)] = (
                    0, 0, None, Decimal("0"), "no_received_records"
                )

        # Request the extended period from the reporting function.
        # Psycopg passes the two dates through the %s placeholders.
        rows = connection.execute(
            f"SELECT {COLUMNS} FROM report_daily_pm25(%s, %s)",
            (date(2013, 2, 28), date(2017, 3, 1)),
        ).fetchall()

        # Check the original results and the 24 additional absent station-days.
        verify("Reporting function including absent days", rows, extended)

    # Reached only if both reporting comparisons completed successfully.
    print(
        "PASS: daily counts, averages, coverage and statuses "
        "match CSV calculations."
    )


# Run main only when this file is executed directly.
if __name__ == "__main__":
    main()