# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import identifier, relation, lookup_dim_key, fmt_datetime7
from sql.units.p5_gold_seed.integration import Integration, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import column_type, seed

COLUMNS = ("purchase_key", "date_key", "supplier_key", "stock_item_key",
           "wwi_purchase_order_id", "ordered_outers", "ordered_quantity",
           "received_outers", "package", "is_order_finalized", "lineage_key")


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff_literal(new_cutoff)
    if new_cutoff < last_cutoff:
        raise ValueError("Cutoff window is reversed")
    target = f"{catalog}.{gold_schema}.fact_purchase"
    source = relation(f"{catalog}.{silver_schema}.purchase_staging")
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.fact_purchase"], replace=True)
        return
    control = Integration(spark, catalog, gold_schema)
    lineage_key = control.start_lineage("Purchase", new_cutoff)
    hwm = high_water_mark(spark, target, "purchase_key")
    views = ("g11_purchase_stage", "g11_purchase_supplier", "g11_purchase_stock_item")
    try:
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {views[0]} AS SELECT * EXCEPT (last_modified_when), "
                  f"{fmt_datetime7('last_modified_when')} AS last_modified_when "
                  f"FROM {source} WHERE _staging_drop = '{drop}'").collect()
        for dimension, facts, view, key, natural_id in (
                ("dim_supplier", views[0], views[1], "supplier_key", "wwi_supplier_id"),
                ("dim_stock_item", views[1], views[2], "stock_item_key", "wwi_stock_item_id")):
            lookup = lookup_dim_key(
                f"{catalog}.{gold_schema}.{dimension}", facts,
                fact_row_key=("_drop", "_staged_order"), key=key,
                business_key=natural_id, natural_id=natural_id,
                timestamp="last_modified_when", output_key=key)
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS {lookup}").collect()
        names = ", ".join(identifier(c) for c in COLUMNS)
        values = {"purchase_key": f"{hwm} + ROW_NUMBER() OVER (ORDER BY _drop, _staged_order)",
                  "lineage_key": str(lineage_key)}
        projection = ", ".join(
            f"CAST({values.get(c, identifier(c))} AS {column_type(c)}) AS {identifier(c)}"
            for c in COLUMNS)
        spark.sql(f"DELETE FROM {relation(target)} WHERE wwi_purchase_order_id IN "
                  f"(SELECT wwi_purchase_order_id FROM {views[0]})").collect()
        spark.sql(f"INSERT INTO {relation(target)} ({names}) "
                  f"SELECT {projection} FROM {views[2]}").collect()
        control.complete_lineage(lineage_key)
    finally:
        for view in reversed(views):
            spark.sql(f"DROP VIEW IF EXISTS {view}").collect()


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
