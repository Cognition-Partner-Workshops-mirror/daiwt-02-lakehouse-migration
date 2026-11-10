# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from parity.parity_check import literal
from sql.framework import identifier, relation
from sql.units.p5_gold_seed.integration import Integration, cutoff_literal
from sql.units.p5_gold_seed.seed import column_type, seed


def snapshot_select(entry, silver, dimension, drop, lineage):
    projection = []
    for column in entry["columns"]:
        if column == "stock_holding_key":
            value = "ROW_NUMBER() OVER (ORDER BY s.wwi_stock_item_id)"
        elif column == "stock_item_key":
            value = "coalesce(d.stock_item_key, 0)"
        elif column == "lineage_key":
            value = str(lineage)
        elif column == "last_cost_price":
            value = "CAST(s.last_cost_price AS STRING)"
        else:
            value = f"s.{identifier(column)}"
        projection.append(f"CAST({value} AS {column_type(column)}) AS {identifier(column)}")
    return (
        "WITH latest AS (SELECT stock_item_key, wwi_stock_item_id, "
        "ROW_NUMBER() OVER (PARTITION BY wwi_stock_item_id ORDER BY valid_to DESC, stock_item_key DESC) AS rn "
        f"FROM {dimension}) SELECT {', '.join(projection)} "
        f"FROM {silver} s LEFT JOIN latest d ON s.wwi_stock_item_id = d.wwi_stock_item_id AND d.rn = 1 "
        f"WHERE s.`drop` = {literal(drop)}")


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff_literal(new_cutoff)
    if last_cutoff > new_cutoff:
        raise ValueError("Reversed window")
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.fact_stock_holding"], replace=True)
        return
    silver = relation(f"{catalog}.{silver_schema}.stock_holding_snapshot")
    if not spark.sql(f"SELECT DISTINCT `drop` FROM {silver} WHERE `drop` = {literal(drop)}").collect():
        raise ValueError("run Stock Holding silver for this drop first")
    integration = Integration(spark, catalog, gold_schema)
    lineage = integration.start_lineage("Stock Holding", new_cutoff)
    entry = next(e for e in contract["entries"] if e["table"] == "gold.fact_stock_holding" and e["drop"] == "D0")
    target = relation(f"{catalog}.{gold_schema}.fact_stock_holding")
    dimension = relation(f"{catalog}.{gold_schema}.dim_stock_item")
    spark.sql(f"CREATE OR REPLACE TABLE {target} USING DELTA AS " +
              snapshot_select(entry, silver, dimension, drop, lineage)).collect()
    integration.complete_lineage(lineage)


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
