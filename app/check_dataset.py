# Check every source observation using the consumer's validation rules.
# This script reads CSV files only; it does not use Kafka or write to PostgreSQL.
import csv
from pathlib import Path

from consumer import prepare_observation


# Find the station CSV files in the folder mounted inside the container.
paths = sorted(Path("/data").glob("PRSA_Data_*.csv"))

if len(paths) != 12:
    raise RuntimeError(f"Expected 12 station files; found {len(paths)}")

# Keep overall counts across all files.
total = 0
invalid = 0

for path in paths:
    # Reset these counts for each station file.
    file_rows = 0
    file_invalid = 0

    with path.open(encoding="utf-8-sig", newline="") as file:
        # Read observations one at a time. Numbering starts at 2 because
        # the first CSV line contains the column names.
        for line_number, row in enumerate(
            csv.DictReader(file), start=2
        ):
            total += 1
            file_rows += 1

            try:
                # Use the same validation function as the real consumer.
                prepare_observation(row)
            except (ValueError, ArithmeticError, KeyError, TypeError) as error:
                # Count rejected rows and continue checking the remaining data.
                invalid += 1
                file_invalid += 1

                # Print details for only the first 10 failures overall.
                if invalid <= 10:
                    print(
                        f"INVALID: {path.name}, "
                        f"line {line_number}: {error}"
                    )

    print(
        f"{path.name}: "
        f"{file_rows} rows, {file_invalid} invalid"
    )

print(f"TOTAL: {total} rows checked; {invalid} invalid")

# Return an error status if any row failed validation.
# Zero failures means the dataset passed these rules, not that sensors are accurate.
if invalid:
    raise SystemExit(1)