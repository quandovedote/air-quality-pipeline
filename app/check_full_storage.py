"""Compare the full source dataset with stored observations.

Read-only check. Requires all 420,768 observations in PostgreSQL.
Run inside Docker Compose with the original CSV files mounted at /data.
"""
import csv
import os
from datetime import datetime
from decimal import Decimal
from itertools import chain, zip_longest
from pathlib import Path

import psycopg


# Map source fields to database columns independently of consumer.py.
FIELDS = [
    ("PM2.5", "pm25"), ("PM10", "pm10"),
    ("SO2", "so2"), ("NO2", "no2"),
    ("CO", "co"), ("O3", "o3"),
    ("TEMP", "temperature"), ("PRES", "pressure"),
    ("DEWP", "dew_point"), ("RAIN", "rainfall"),
    ("wd", "wind_direction"), ("WSPM", "wind_speed"),
]


def expected_values(row):
    """Build the expected database values directly from a CSV row."""
    timestamp = datetime(
        int(row["year"]), int(row["month"]),
        int(row["day"]), int(row["hour"]),
    )
    values = [row["station"].strip(), timestamp]

    for source, _ in FIELDS:
        value = row[source].strip()
        if value in ("", "NA"):
            values.append(None)
        elif source == "wd":
            values.append(value)
        else:
            values.append(Decimal(value))

    return tuple(values)


def main():
    paths = sorted(Path("/data").glob("PRSA_Data_*.csv"))
    if len(paths) != 12:
        raise RuntimeError(f"Expected 12 CSV files; found {len(paths)}")

    columns = "station, observed_at, " + ", ".join(
        database for _, database in FIELDS
    )
    checked = 0
    mismatches = 0
    stations = set()
    missing_pm25 = 0

    with psycopg.connect(
        host=os.getenv("PGHOST", "postgres"),
        dbname=os.getenv("PGDATABASE", "air_quality"),
        user=os.getenv("PGUSER", "air_quality"),
        password=os.getenv("PGPASSWORD", "local_dev_password"),
        connect_timeout=10,
    ) as connection:
        # Prevent this verification transaction from changing database data.
        connection.execute("SET TRANSACTION READ ONLY")

        for index, path in enumerate(paths):
            with path.open(encoding="utf-8-sig", newline="") as source:
                reader = csv.DictReader(source)
                first = next(reader, None)
                if first is None:
                    raise RuntimeError(f"Empty source file: {path.name}")

                station = first["station"].strip()
                if station in stations:
                    raise RuntimeError(f"Repeated station file: {station}")
                stations.add(station)
                station_checked = 0
                station_mismatches = 0

                # Stream one station's database rows without loading
                # the complete dataset into Python memory.
                with connection.cursor(name=f"verify_{index}") as cursor:
                    cursor.execute(
                        f"SELECT {columns} FROM observations "
                        "WHERE station = %s ORDER BY observed_at",
                        (station,),
                    )
                    source_rows = (
                        expected_values(row)
                        for row in chain([first], reader)
                    )

                    # Unequal lengths also produce mismatches.
                    for expected, actual in zip_longest(source_rows, cursor):
                        checked += 1
                        station_checked += 1
                        if expected is not None and expected[2] is None:
                            missing_pm25 += 1
                        if expected != actual:
                            mismatches += 1
                            station_mismatches += 1
                            if mismatches <= 5:
                                print(
                                    "MISMATCH:", station,
                                    "source=", expected,
                                    "database=", actual,
                                    flush=True,
                                )

                print(
                    f"{station}: {station_checked} rows compared, "
                    f"{station_mismatches} mismatches",
                    flush=True,
                )

        database_count = connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()[0]

    print(f"Total rows compared: {checked}")
    print(f"Database rows: {database_count}")
    print(f"Source missing PM2.5 values: {missing_pm25}")
    print(f"Total mismatches: {mismatches}")

    if checked != 420768 or database_count != 420768:
        raise RuntimeError("Expected 420,768 source and database observations")
    if mismatches:
        raise RuntimeError("Source and database differ")

    print("PASS: all source observations match stored keys and measurements.")


if __name__ == "__main__":
    main()
