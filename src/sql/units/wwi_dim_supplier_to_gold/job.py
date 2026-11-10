# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import identifier, relation, scd2
from sql.units.p5_gold_seed.integration import Integration, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import column_type, seed

COLUMNS = ("wwi_supplier_id", "supplier", "category", "primary_contact",
           "supplier_reference", "payment_days", "postal_code", "valid_from", "valid_to")


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff_literal(new_cutoff)
    if new_cutoff < last_cutoff:
        raise ValueError("Cutoff window is reversed")
    target = f"{catalog}.{gold_schema}.dim_supplier"
    source = relation(f"{catalog}.{silver_schema}.supplier_changes")
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.dim_supplier"], replace=True)
        return
    control = Integration(spark, catalog, gold_schema)
    lineage_key = control.start_lineage("Supplier", new_cutoff)
    hwm = high_water_mark(spark, target, "supplier_key")
    names = ", ".join(f"CAST({identifier(c)} AS {column_type(c)}) AS {identifier(c)}"
                      for c in COLUMNS)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW g07_supplier_stage AS SELECT {names}, "
              f"{lineage_key} AS lineage_key FROM {source}").collect()
    try:
        for statement in scd2(target, "g07_supplier_stage", key="supplier_key",
                              business_key="wwi_supplier_id", columns=(*COLUMNS, "lineage_key"),
                              staging_order=COLUMNS, key_high_water_mark=hwm):
            spark.sql(statement).collect()
        control.complete_lineage(lineage_key)
    finally:
        spark.sql("DROP VIEW IF EXISTS g07_supplier_stage").collect()


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
