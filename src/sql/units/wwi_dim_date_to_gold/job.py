# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.units.p5_gold_seed.seed import seed


def run(spark, contract, *, catalog, gold_schema, golden_path, drop):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    seed(spark, contract, catalog, gold_schema, golden_path,
         tables=["gold.dim_date"], replace=True)


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "golden_path", "drop")})
