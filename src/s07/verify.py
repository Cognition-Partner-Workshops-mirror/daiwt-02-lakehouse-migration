# Databricks notebook source
from transaction_type import COLUMNS, assert_parity, expected_rows

for name, default in (("catalog", "daiwt_02_mig_dev"), ("silver_schema", "silver_pr_8"),
                      ("golden_path", "/Volumes/daiwt_02_mig_prod/landing/golden"), ("drop", "D0")):
    dbutils.widgets.text(name, default)
catalog, silver, golden, drop = [dbutils.widgets.get(n) for n in
    ("catalog", "silver_schema", "golden_path", "drop")]


def golden_rows(d):
    return [r.asDict() for r in spark.read.options(header=True, inferSchema=False, nullValue="\\N").csv(
        f"{golden}/outputs/{d}/tables/Dimension.Transaction_Type.csv").collect()]


expected = expected_rows(golden_rows(drop), None if drop == "D0" else golden_rows(f"D{int(drop[1:]) - 1}"))
actual = [tuple(None if r[c] is None else str(r[c]) for c in COLUMNS)
          for r in spark.table(f"{catalog}.{silver}.transaction_type_changes").collect()]
quarantine_count = spark.table(f"{catalog}.{silver}.transaction_type_changes_quarantine").count()
assert_parity(actual, expected, quarantine_count)
if drop == "D0":
    staged_order = sorted(actual, key=lambda row: (row[2], int(row[0])))
    key_order = [tuple(r[c] for c in COLUMNS) for r in sorted(golden_rows(drop), key=lambda r: int(r["transaction_type_key"]))
                 if r["transaction_type_key"] != "0"]
    assert staged_order == key_order, "S07 staged ordering differs from golden key ordering"
print(f"S07 {drop} PASS: staged={len(actual)}, expected={sum(expected.values())}, quarantine={quarantine_count}")
