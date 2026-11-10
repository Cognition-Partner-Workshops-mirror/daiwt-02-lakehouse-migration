# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from parity.parity_check import literal
from sql.framework import fmt_datetime7, identifier, lookup_dim_key, relation
from sql.units.p5_gold_seed.integration import Integration, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import column_type, seed

DIMENSIONS = ("stock_item", "customer", "supplier", "transaction_type")
ROW_KEY = "wwi_stock_item_transaction_id"


def merge_source(entry, staging, target, high_water, lineage):
    columns = [c for c in entry["columns"] if c != "movement_key"]
    projection = ", ".join(
        f"CAST({lineage if c == 'lineage_key' else 's.' + identifier(c)} AS {column_type(c)}) AS {identifier(c)}"
        for c in columns)
    return (
        f"WITH staged AS (SELECT {projection} FROM {staging} s) "
        f"SELECT t.movement_key, s.* FROM staged s JOIN {target} t "
        f"ON s.{ROW_KEY} = t.{ROW_KEY} UNION ALL "
        f"SELECT {high_water} + ROW_NUMBER() OVER (ORDER BY s.{ROW_KEY}) AS movement_key, s.* "
        f"FROM staged s WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE s.{ROW_KEY} = t.{ROW_KEY})")


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
             tables=["gold.fact_movement"], replace=True)
        return
    window = relation(f"{catalog}.{silver_schema}.movement_window")
    if not spark.sql(
            f"SELECT 1 FROM {window} WHERE _drop = {literal(drop)} "
            f"AND last_cutoff = CAST({literal(last_cutoff)} AS TIMESTAMP_NTZ) "
            f"AND new_cutoff = CAST({literal(new_cutoff)} AS TIMESTAMP_NTZ)").collect():
        raise ValueError("run Movement silver for this window first")
    silver = relation(f"{catalog}.{silver_schema}.movement_changes")
    spark.sql("CREATE OR REPLACE TEMP VIEW movement_stage AS SELECT * EXCEPT(last_modified_when), "
              f"{fmt_datetime7('last_modified_when', source='timestamp')} AS last_modified_when "
              f"FROM {silver} WHERE _drop = {literal(drop)}").collect()
    stage = "movement_stage"
    for dim in DIMENSIONS:
        query = lookup_dim_key(f"{catalog}.{gold_schema}.dim_{dim}", stage,
                               fact_row_key=[ROW_KEY], key=f"{dim}_key",
                               business_key=f"wwi_{dim}_id", natural_id=f"wwi_{dim}_id",
                               timestamp="last_modified_when", output_key=f"{dim}_key")
        spark.sql(f"CREATE OR REPLACE TEMP VIEW movement_lookup_{dim} AS {query}").collect()
        stage = f"movement_lookup_{dim}"
    integration = Integration(spark, catalog, gold_schema)
    lineage = integration.start_lineage("Movement", new_cutoff)
    entry = next(e for e in contract["entries"] if e["table"] == "gold.fact_movement" and e["drop"] == "D0")
    target_name = f"{catalog}.{gold_schema}.fact_movement"
    target = relation(target_name)
    source = merge_source(entry, stage, target,
                          high_water_mark(spark, target_name, "movement_key"), lineage)
    updates = ", ".join(f"t.{identifier(c)} = s.{identifier(c)}" for c in entry["columns"]
                        if c not in ("movement_key", ROW_KEY))
    columns = ", ".join(identifier(c) for c in entry["columns"])
    values = ", ".join(f"s.{identifier(c)}" for c in entry["columns"])
    spark.sql(f"MERGE INTO {target} t USING ({source}) s ON t.{ROW_KEY} = s.{ROW_KEY} "
              f"WHEN MATCHED THEN UPDATE SET {updates} "
              f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})").collect()
    integration.complete_lineage(lineage)


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
