# Test the consumer's validation rules using real and modified observations.
# This script does not send Kafka messages or write to PostgreSQL.
import csv
from decimal import Decimal
from pathlib import Path

from consumer import prepare_observation


path = Path(
    "/data/PRSA_Data_Aotizhongxin_20130301-20170228.csv"
)

# Read the first source observation as the starting point for our tests.
with path.open(encoding="utf-8-sig", newline="") as file:
    original = next(csv.DictReader(file))

# Check that the real observation is accepted and keeps its expected values.
# Positions match the prepared row: 0 = station, 2 = PM2.5, 8 = temperature.
# An assert stops the test if its condition is false.
values = prepare_observation(original)
assert values[0] == "Aotizhongxin"
assert values[2] == Decimal("4")
assert values[8] == Decimal("-0.7")
print("PASS: original values and negative temperature accepted")

# Change a copy of the row; the original observation and CSV stay unchanged.
# Missing PM2.5 should become None, which database inserts later store as NULL.
missing = original.copy()
missing["PM2.5"] = "NA"
assert prepare_observation(missing)[2] is None
print("PASS: missing PM2.5 becomes None")

# Each pair contains a field and an invalid value that should be rejected.
invalid_cases = [
    ("station", "UnknownStation"),
    ("hour", "25"),
    ("PM2.5", "-1"),
    ("PM2.5", "NaN"),
    ("PM10", "not-a-number"),
    ("PRES", "0"),
    ("wd", "INVALID"),
]

for field, value in invalid_cases:
    # Start with a fresh copy so each test changes only one field.
    row = original.copy()
    row[field] = value

    try:
        prepare_observation(row)
    except (ValueError, ArithmeticError):
        # Rejection is the expected result for these invalid examples.
        print(f"PASS: rejected {field}={value}")
    else:
        # No exception means validation incorrectly accepted the test value.
        raise AssertionError(
            f"Invalid value accepted: {field}={value}"
        )

# Reached only if every check above completed successfully.
print("All validation checks passed")