import csv
from pathlib import Path

from consumer import prepare_observation


paths = sorted(Path("/data").glob("PRSA_Data_*.csv"))

if len(paths) != 12:
    raise RuntimeError(f"Expected 12 station files; found {len(paths)}")

total = 0
invalid = 0

for path in paths:
    file_rows = 0
    file_invalid = 0

    with path.open(encoding="utf-8-sig", newline="") as file:
        for line_number, row in enumerate(
            csv.DictReader(file), start=2
        ):
            total += 1
            file_rows += 1

            try:
                prepare_observation(row)
            except (ValueError, ArithmeticError, KeyError, TypeError) as error:
                invalid += 1
                file_invalid += 1

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

if invalid:
    raise SystemExit(1)