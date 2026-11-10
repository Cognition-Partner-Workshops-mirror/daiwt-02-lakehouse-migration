# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import fmt_datetime7, identifier, lookup_dim_key, relation
from sql.units.p5_gold_seed.integration import Integration, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import column_type, seed

LINE_MATCH = [
    ("WWI Invoice ID", "InvoiceID"), ("WWI Stock Item ID", "StockItemID"),
    ("Description", "Description"), ("Quantity", "Quantity"), ("Unit Price", "UnitPrice"),
    ("Tax Rate", "TaxRate"), ("Tax Amount", "TaxAmount"), ("Profit", "LineProfit"),
    ("Total Including Tax", "ExtendedPrice"),
]
STAGE_COLUMNS = [
    ("Invoice Date Key", "invoice_date_key"), ("Delivery Date Key", "delivery_date_key"),
    ("WWI Invoice ID", "wwi_invoice_id"), ("Description", "description"), ("Package", "package"),
    ("Quantity", "quantity"), ("Unit Price", "unit_price"), ("Tax Rate", "tax_rate"),
    ("Total Excluding Tax", "total_excluding_tax"), ("Tax Amount", "tax_amount"),
    ("Profit", "profit"), ("Total Including Tax", "total_including_tax"),
    ("Total Dry Items", "total_dry_items"), ("Total Chiller Items", "total_chiller_items"),
    ("WWI City ID", "_city_id"), ("WWI Customer ID", "_customer_id"),
    ("WWI Bill To Customer ID", "_bill_to_customer_id"), ("WWI Stock Item ID", "_stock_item_id"),
    ("WWI Salesperson ID", "_salesperson_id"),
]
LOOKUPS = [
    ("dim_city", "city_key", "wwi_city_id", "_city_id", "city_key"),
    ("dim_customer", "customer_key", "wwi_customer_id", "_customer_id", "customer_key"),
    ("dim_customer", "customer_key", "wwi_customer_id", "_bill_to_customer_id", "bill_to_customer_key"),
    ("dim_stock_item", "stock_item_key", "wwi_stock_item_id", "_stock_item_id", "stock_item_key"),
    ("dim_employee", "employee_key", "wwi_employee_id", "_salesperson_id", "salesperson_key"),
]


def q(name):
    return "`" + name.replace("`", "``") + "`"


def stage_select(silver, lines):
    """Staged Sale rows in staging order, each carrying its source invoice line ID."""
    signature = ", ".join(q(s) for s, _ in LINE_MATCH)
    line_signature = ", ".join(q(b) for _, b in LINE_MATCH)
    projection = ", ".join(f"s.{q(s)} AS {identifier(t)}" for s, t in STAGE_COLUMNS)
    match = " AND ".join(f"s.{q(s)} <=> l.{q(b)}" for s, b in LINE_MATCH)
    ts = fmt_datetime7("s.`Last Modified When`", source="timestamp")
    return (
        f"WITH s AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY {signature} ORDER BY {signature}) AS _match "
        f"FROM {silver}), "
        f"l AS (SELECT {line_signature}, InvoiceLineID, ROW_NUMBER() OVER (PARTITION BY {line_signature} "
        f"ORDER BY InvoiceLineID) AS _match FROM {lines}) "
        f"SELECT {projection}, {ts} AS _ts, l.InvoiceLineID AS _line_id "
        f"FROM s LEFT JOIN l ON {match} AND s._match = l._match")


def insert_select(entry, source, high_water_mark_value, lineage):
    projection = []
    for column in entry["columns"]:
        if column == "sale_key":
            value = f"{high_water_mark_value} + ROW_NUMBER() OVER (ORDER BY wwi_invoice_id, _line_id)"
        elif column == "lineage_key":
            value = str(lineage)
        elif column_type(column).startswith("DECIMAL"):
            value = f"CAST({identifier(column)} AS STRING)"
        else:
            value = identifier(column)
        projection.append(f"CAST({value} AS {column_type(column)}) AS {identifier(column)}")
    return f"SELECT {', '.join(projection)} FROM {source}"


def run(spark, contract, *, catalog, gold_schema, silver_schema, bronze_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff_literal(new_cutoff)
    if last_cutoff > new_cutoff:
        raise ValueError("Reversed window")
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.fact_sale"], replace=True)
        return
    log = relation(f"{catalog}.{bronze_schema}.bronze_replay_log")
    latest = spark.sql(f"SELECT DISTINCT `drop` FROM {log} WHERE drop_index = "
                       f"(SELECT max(drop_index) FROM {log})").collect()
    if [row[0] for row in latest] != [drop]:
        raise ValueError("run bronze replay and Sale silver for this drop first")
    spark.sql("CREATE OR REPLACE TEMP VIEW sale_gold_stage_0 AS " + stage_select(
        relation(f"{catalog}.{silver_schema}.sale_changes"),
        relation(f"{catalog}.{bronze_schema}.sales_invoice_lines"))).collect()
    unmatched = spark.sql("SELECT count(*) FROM sale_gold_stage_0 WHERE _line_id IS NULL").first()[0]
    if unmatched:
        raise ValueError("Staged Sale rows without a source invoice line")
    for index, (dimension, key, business_key, natural_id, output_key) in enumerate(LOOKUPS, 1):
        spark.sql(f"CREATE OR REPLACE TEMP VIEW sale_gold_stage_{index} AS " + lookup_dim_key(
            f"{catalog}.{gold_schema}.{dimension}", f"sale_gold_stage_{index - 1}",
            fact_row_key=["_line_id"], key=key, business_key=business_key,
            natural_id=natural_id, timestamp="_ts", output_key=output_key)).collect()
    staged = f"sale_gold_stage_{len(LOOKUPS)}"
    integration = Integration(spark, catalog, gold_schema)
    lineage = integration.start_lineage("Sale", new_cutoff)
    entry = next(e for e in contract["entries"] if e["table"] == "gold.fact_sale" and e["drop"] == "D0")
    target = f"{catalog}.{gold_schema}.fact_sale"
    hwm = high_water_mark(spark, target, "sale_key")
    spark.sql(f"DELETE FROM {relation(target)} WHERE wwi_invoice_id IN "
              f"(SELECT wwi_invoice_id FROM {staged})").collect()
    columns = ", ".join(identifier(c) for c in entry["columns"])
    spark.sql(f"INSERT INTO {relation(target)} ({columns}) " +
              insert_select(entry, staged, hwm, lineage)).collect()
    integration.complete_lineage(lineage)


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "bronze_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
