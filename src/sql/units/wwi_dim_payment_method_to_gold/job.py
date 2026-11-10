# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import fmt_datetime7, relation, scd2
from sql.units.p5_gold_seed.integration import cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import seed


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff = cutoff_literal(new_cutoff)
    if new_cutoff < last_cutoff:
        raise ValueError("Cutoff window is reversed")
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.dim_payment_method"], replace=True)
        return
    target = f"{catalog}.{gold_schema}.dim_payment_method"
    lineage = relation(f"{catalog}.{gold_schema}.integration_lineage")
    rows = spark.sql(
        f"SELECT lineage_key FROM {lineage} WHERE table_name = 'Payment Method' "
        f"AND source_system_cutoff_time = {cutoff}").collect()
    if len(rows) != 1:
        raise ValueError("Expected exactly one Payment Method lineage row")
    lineage_key = int(rows[0][0])
    if spark.sql(
            f"SELECT 1 FROM {relation(target)} WHERE lineage_key = {lineage_key} LIMIT 1").collect():
        return
    staging = "dim_payment_method_staging"
    source = relation(f"{catalog}.{silver_schema}.payment_method_changes")
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {relation(staging)} AS SELECT "
        f"CAST(wwi_payment_method_id AS BIGINT) AS wwi_payment_method_id, "
        f"CAST(payment_method AS STRING) AS payment_method, "
        f"{fmt_datetime7('valid_from')} AS valid_from, "
        f"{fmt_datetime7('valid_to')} AS valid_to, "
        f"CAST({lineage_key} AS BIGINT) AS lineage_key FROM {source}").collect()
    hwm = high_water_mark(spark, target, "payment_method_key")
    for statement in scd2(
            target, staging, key="payment_method_key", business_key="wwi_payment_method_id",
            columns=("wwi_payment_method_id", "payment_method", "valid_from", "valid_to", "lineage_key"),
            staging_order=("valid_from", "wwi_payment_method_id", "payment_method"),
            key_high_water_mark=hwm):
        spark.sql(statement).collect()


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
