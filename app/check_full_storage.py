# Compare every source observation with its stored PostgreSQL values.
# This script reads data but does not insert, update or delete observations.
# All 420,768 observations must already be stored in PostgreSQL.
# Run inside Docker Compose with the original CSV files mounted at /data.
import csv
import os
from datetime import datetime
from decimal import Decimal
from itertools import chain, zip_longest
from pathlib import Path

import psycopg


# Each pair links a CSV field to its corresponding database column.
# Keep this mapping separate from consumer.py to check storage independently.
FIELDS = [
    ("PM2.5", "pm25"), ("PM10", "pm10"),
    ("SO2", "so2"), ("NO2", "no2"),
    ("CO", "co"), ("O3", "o3"),
    ("TEMP", "temperature"), ("PRES", "pressure"),
    ("DEWP", "dew_point"), ("RAIN", "rainfall"),
    ("wd", "wind_direction"), ("WSPM", "wind_speed"),
]


def expected_values(row):
    # Build the original observation timestamp from the CSV date fields.
    timestamp = datetime(
        int(row["year"]), int(row["month"]),
        int(row["day"]), int(row["hour"]),
    )

    # The first two values identify the station and observation time.
    values = [row["station"].strip(), timestamp]

    # Convert source measurements into their expected database values.
    # The database column name is not needed inside this loop.
    for source, _ in FIELDS:
        value = row[source].strip()

        # Missing source readings should match SQL NULL, returned as None.
        if value in ("", "NA"):
            values.append(None)

        # Wind direction is text; the other measurements are numeric.
        elif source == "wd":
            values.append(value)
        else:
            values.append(Decimal(value))

    # Return values in the same order as the database SELECT columns.
    return tuple(values)


def main():
    # Find the 12 station CSV files mounted inside the container.
    paths = sorted(Path("/data").glob("PRSA_Data_*.csv"))
    if len(paths) != 12:
        raise RuntimeError(f"Expected 12 CSV files; found {len(paths)}")

    # Build the SELECT column list using the database names in FIELDS.
    columns = "station, observed_at, " + ", ".join(
        database for _, database in FIELDS
    )

    # Keep overall comparison counts and record each station once.
    checked = 0
    mismatches = 0
    stations = set()
    missing_pm25 = 0

    # Use environment settings when provided, with local prototype defaults.
    # The hostname "postgres" works inside the Docker Compose network.
    with psycopg.connect(
        host=os.getenv("PGHOST", "postgres"),
        dbname=os.getenv("PGDATABASE", "air_quality"),
        user=os.getenv("PGUSER", "air_quality"),
        password=os.getenv("PGPASSWORD", "local_dev_password"),
        connect_timeout=10,
    ) as connection:
        # Prevent this verification transaction from changing database data.
        connection.execute("SET TRANSACTION READ ONLY")

        # Give each file an index so its database cursor has a unique name.
        for index, path in enumerate(paths):
            with path.open(encoding="utf-8-sig", newline="") as source:
                reader = csv.DictReader(source)

                # Read the first observation to identify the station.
                # next returns None if the file contains no observations.
                first = next(reader, None)
                if first is None:
                    raise RuntimeError(f"Empty source file: {path.name}")

                # Reject multiple source files starting with the same station.
                station = first["station"].strip()
                if station in stations:
                    raise RuntimeError(f"Repeated station file: {station}")
                stations.add(station)

                # Reset the comparison counts for this station.
                station_checked = 0
                station_mismatches = 0

                # A named cursor retrieves database rows progressively.
                # This avoids loading the complete dataset into Python memory.
                with connection.cursor(name=f"verify_{index}") as cursor:
                    # Select this station's observations in timestamp order.
                    # Psycopg passes the station through the %s placeholder.
                    cursor.execute(
                        f"SELECT {columns} FROM observations "
                        "WHERE station = %s ORDER BY observed_at",
                        (station,),
                    )

                    # Put the first observation back before the remaining rows.
                    # Convert each CSV row only when the comparison needs it.
                    # The original station CSV files must be in timestamp order.
                    source_rows = (
                        expected_values(row)
                        for row in chain([first], reader)
                    )

                    # Compare source and database observations side by side.
                    # zip_longest continues if one side has additional rows,
                    # using None for the missing side so the difference is found.
                    for expected, actual in zip_longest(source_rows, cursor):
                        checked += 1
                        station_checked += 1

                        # Position 2 contains PM2.5 after station and timestamp.
                        # Count missing PM2.5 readings in the source data.
                        if expected is not None and expected[2] is None:
                            missing_pm25 += 1

                        # Compare the complete tuples, including missing values.
                        if expected != actual:
                            mismatches += 1
                            station_mismatches += 1

                            # Show only the first five mismatches overall.
                            if mismatches <= 5:
                                print(
                                    "MISMATCH:", station,
                                    "source=", expected,
                                    "database=", actual,
                                    flush=True,
                                )

                # Display progress after completing each station.
                print(
                    f"{station}: {station_checked} rows compared, "
                    f"{station_mismatches} mismatches",
                    flush=True,
                )

        # Check the whole table count as well as the per-station comparisons.
        # This also detects extra rows belonging to an unexpected station.
        database_count = connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()[0]

    # Display the overall results after closing the database connection.
    print(f"Total rows compared: {checked}")
    print(f"Database rows: {database_count}")
    print(f"Source missing PM2.5 values: {missing_pm25}")
    print(f"Total mismatches: {mismatches}")

    # Fail if either count differs from the complete dataset size.
    if checked != 420768 or database_count != 420768:
        raise RuntimeError("Expected 420,768 source and database observations")

    # Matching counts are not enough: every compared value must also match.
    if mismatches:
        raise RuntimeError("Source and database differ")

    # Reached only when the row counts and all compared values match.
    print("PASS: all source observations match stored keys and measurements.")


# Run main only when this file is executed directly.
if __name__ == "__main__":
    main()