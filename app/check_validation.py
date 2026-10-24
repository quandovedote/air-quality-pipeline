import csv
from decimal import Decimal
from pathlib import Path

from consumer import prepare_observation


path = Path(
    "/data/PRSA_Data_Aotizhongxin_20130301-20170228.csv"
)

with path.open(encoding="utf-8-sig", newline="") as file:
    original = next(csv.DictReader(file))

# Genuine source observation must pass.
values = prepare_observation(original)
assert values[0] == "Aotizhongxin"
assert values[2] == Decimal("4")
assert values[8] == Decimal("-0.7")
print("PASS: original values and negative temperature accepted")

# Missing PM2.5 must remain missing, not become zero.
missing = original.copy()
missing["PM2.5"] = "NA"
assert prepare_observation(missing)[2] is None
print("PASS: missing PM2.5 becomes None")

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
    row = original.copy()
    row[field] = value

    try:
        prepare_observation(row)
    except (ValueError, ArithmeticError):
        print(f"PASS: rejected {field}={value}")
    else:
        raise AssertionError(
            f"Invalid value accepted: {field}={value}"
        )

print("All validation checks passed")