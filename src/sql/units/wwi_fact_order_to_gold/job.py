# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from parity.parity_check import literal
from sql.framework import identifier, lookup_dim_key, relation
from sql.units.p5_gold_seed.integration import Integration, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import column_type, seed


LOOKUPS = (
    ("city", "city_key", "wwi_city_id", "wwi_city_id"),
    ("customer", "customer_key", "wwi_customer_id", "wwi_customer_id"),
    ("stock_item", "stock_item_key", "wwi_stock_item_id", "wwi_stock_item_id"),
    ("employee", "salesperson_key", "wwi_employee_id", "wwi_salesperson_id"),
    ("employee", "picker_key", "wwi_employee_id", "wwi_picker_id"),
)


def order_select(entry, silver, catalog, gold_schema, drop, lineage, hwm):
    views = [f"staged AS (SELECT * FROM {silver} WHERE _drop = {literal(drop)})"]
    previous = "staged"
    for index, (dimension, output, business, natural) in enumerate(LOOKUPS):
        name = f"lookup_{index}"
        key = "employee_key" if dimension == "employee" else output
        select = lookup_dim_key(f"{catalog}.{gold_schema}.dim_{dimension}", previous,
                                fact_row_key=["_stage_seq"], key=key, business_key=business,
                                natural_id=natural, timestamp="_last_modified_when_text",
                                output_key=output)
        views.append(f"{name} AS ({select})")
        previous = name
    projection = []
    for column in entry["columns"]:
        if column == "order_key":
            value = f"{hwm} + ROW_NUMBER() OVER (ORDER BY _stage_seq)"
        elif column == "lineage_key":
            value = str(lineage)
        else:
            value = identifier(column)
        dtype = column_type(column)
        if dtype.startswith("DECIMAL"):
            value = f"CAST({value} AS STRING)"
        projection.append(f"CAST({value} AS {dtype}) AS {identifier(column)}")
    return "WITH " + ", ".join(views) + f" SELECT {', '.join(projection)} FROM {previous}"


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
             tables=["gold.fact_order"], replace=True)
        return
    silver = relation(f"{catalog}.{silver_schema}.order_changes")
    window = relation(f"{catalog}.{silver_schema}.order_extract_window")
    if not spark.sql(f"SELECT * FROM {window} WHERE drop = {literal(drop)} "
                     f"AND last_cutoff = {literal(last_cutoff)} AND new_cutoff = {literal(new_cutoff)}").collect():
        raise ValueError("run Order silver for this drop and window first")
    integration = Integration(spark, catalog, gold_schema)
    lineage = integration.start_lineage("Order", new_cutoff)
    entry = next(e for e in contract["entries"] if e["table"] == "gold.fact_order" and e["drop"] == "D0")
    target_name = f"{catalog}.{gold_schema}.fact_order"
    target = relation(target_name)
    hwm = high_water_mark(spark, target_name, "order_key")
    spark.sql(f"DELETE FROM {target} WHERE wwi_order_id IN "
              f"(SELECT wwi_order_id FROM {silver} WHERE _drop = {literal(drop)})").collect()
    columns = ", ".join(identifier(c) for c in entry["columns"])
    spark.sql(f"INSERT INTO {target} ({columns}) " +
              order_select(entry, silver, catalog, gold_schema, drop, lineage, hwm)).collect()
    integration.complete_lineage(lineage)


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
