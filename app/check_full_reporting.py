"""Compare daily SQL reports with independent source CSV calculations.

Read-only check. Requires the complete dataset in PostgreSQL.
"""
import csv
import os
from collections import Counter
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import psycopg


COLUMNS = """
    station, observation_date, received_hours, valid_pm25_hours,
    available_readings_avg_pm25, coverage_percent, coverage_status
"""


def source_summaries():
    """Calculate daily counts, averages and coverage directly from CSV."""
    paths = sorted(Path("/data").glob("PRSA_Data_*.csv"))
    if len(paths) != 12:
        raise RuntimeError(f"Expected 12 CSV files; found {len(paths)}")

    daily = {}
    stations = set()

    for path in paths:
        with path.open(encoding="utf-8-sig", newline="") as source:
            for row in csv.DictReader(source):
                station = row["station"].strip()
                stations.add(station)
                day = date(
                    int(row["year"]), int(row["month"]), int(row["day"])
                )
                totals = daily.setdefault(
                    (station, day), [0, 0, Decimal("0")]
                )
                totals[0] += 1
                value = row["PM2.5"].strip()
                if value not in ("", "NA"):
                    totals[1] += 1
                    totals[2] += Decimal(value)

    expected = {}
    for key, (received, valid, total) in daily.items():
        average = (
            (total / valid).quantize(
                Decimal("0.001"), rounding=ROUND_HALF_UP
            )
            if valid else None
        )
        coverage = (Decimal(valid) * 100 / 24).quantize(
            Decimal("0.1"), rounding=ROUND_HALF_UP
        )
        status = (
            "complete" if valid == 24
            else "no_valid_measurements" if valid == 0
            else "incomplete"
        )
        expected[key] = (received, valid, average, coverage, status)

    if len(stations) != 12 or len(expected) != 17532:
        raise RuntimeError("Unexpected source station or station-day count")

    return stations, expected


def verify(label, rows, expected):
    """Check keys and every returned reporting value."""
    actual = {tuple(row[:2]): tuple(row[2:]) for row in rows}
    if len(rows) != len(actual):
        raise RuntimeError(f"{label}: duplicate station-days")

    differences = [
        key for key in actual.keys() | expected.keys()
        if key not in actual or key not in expected
        or actual[key] != expected[key]
    ]
    if differences:
        key = sorted(differences)[0]
        raise RuntimeError(
            f"{label}: {len(differences)} mismatches. "
            f"Example {key}: CSV={expected.get(key)}, "
            f"SQL={actual.get(key)}"
        )

    print(
        f"PASS: {label}: {len(actual)} station-days, zero mismatches",
        flush=True,
    )
    print(
        "Statuses:",
        dict(Counter(value[-1] for value in actual.values())),
        flush=True,
    )


def main():
    stations, expected = source_summaries()

    with psycopg.connect(
        host=os.getenv("PGHOST", "postgres"),
        dbname=os.getenv("PGDATABASE", "air_quality"),
        user=os.getenv("PGUSER", "air_quality"),
        password=os.getenv("PGPASSWORD", "local_dev_password"),
        connect_timeout=10,
    ) as connection:
        connection.execute("SET TRANSACTION READ ONLY")

        rows = connection.execute(
            f"SELECT {COLUMNS} FROM daily_pm25_summary"
        ).fetchall()
        verify("Daily summary view", rows, expected)

        # Request one day before and after the dataset to verify
        # that absent station-days have zero counts and a NULL average.
        extended = dict(expected)
        for station in stations:
            for day in (date(2013, 2, 28), date(2017, 3, 1)):
                extended[(station, day)] = (
                    0, 0, None, Decimal("0"), "no_received_records"
                )

        rows = connection.execute(
            f"SELECT {COLUMNS} FROM report_daily_pm25(%s, %s)",
            (date(2013, 2, 28), date(2017, 3, 1)),
        ).fetchall()
        verify("Reporting function including absent days", rows, extended)

    print(
        "PASS: daily counts, averages, coverage and statuses "
        "match CSV calculations."
    )


if __name__ == "__main__":
    main()
