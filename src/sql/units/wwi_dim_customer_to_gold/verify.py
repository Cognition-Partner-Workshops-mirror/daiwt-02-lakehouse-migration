# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from parity.parity_check import compare_entry, select_entries


class SparkWarehouse:
    def __init__(self, spark):
        self.spark = spark

    def sql(self, statement):
        return self.spark.sql(statement).collect()


def run(spark, contract, *, catalog, gold_schema, golden_path, drop):
    entry = next(e for e in select_entries(contract, drop) if e["table"] == "gold.dim_customer")
    result = compare_entry(SparkWarehouse(spark), entry, golden_path, catalog, gold_schema)
    print(json.dumps(dict(table=entry["table"], drop=drop, **result)))
    if result["status"] != "PASS":
        raise RuntimeError("dim_customer parity failed: " + result["reason"])
    return result


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "golden_path", "drop")})
