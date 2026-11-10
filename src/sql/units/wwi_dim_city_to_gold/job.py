# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import identifier, relation, scd2
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
             tables=["gold.dim_city"], replace=True)
        return
    lineage = spark.sql(
        f"SELECT lineage_key FROM {relation(f'{catalog}.{gold_schema}.integration_lineage')} "
        f"WHERE table_name = 'City' AND source_system_cutoff_time = {cutoff}").collect()
    if len(lineage) != 1:
        raise ValueError("Expected one P5 City lineage row for the run cutoff")
    lineage_key = int(lineage[0][0])
    entry = next(e for e in contract["entries"]
                 if e["table"] == "gold.dim_city" and e["drop"] == drop)
    columns = [c for c in entry["columns"] if c != "city_key"]
    projection = ", ".join(
        f"{lineage_key} AS lineage_key" if c == "lineage_key" else identifier(c)
        for c in columns)
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW city_gold_stage AS SELECT {projection}, _staged_order "
        f"FROM {relation(f'{catalog}.{silver_schema}.city_changes')} WHERE _drop = '{drop}'").collect()
    target = f"{catalog}.{gold_schema}.dim_city"
    for statement in scd2(
            target, "city_gold_stage", key="city_key", business_key="wwi_city_id",
            columns=columns, staging_order=["_staged_order"],
            key_high_water_mark=high_water_mark(spark, target, "city_key")):
        spark.sql(statement).collect()


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
