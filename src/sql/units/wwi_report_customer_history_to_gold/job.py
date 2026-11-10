# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import relation

COLUMNS = ["customer_key", "wwi_customer_id", "customer", "valid_from", "valid_to"]


def report_sql(catalog, gold_schema):
    source = relation(f"{catalog}.{gold_schema}.dim_customer")
    target = relation(f"{catalog}.{gold_schema}.report_customer_history")
    # customer-history: SELECT [Customer Key],[WWI Customer ID],Customer,[Valid From],[Valid To]
    # FROM Dimension.Customer ORDER BY [Customer Key]
    return (f"CREATE OR REPLACE TABLE {target} USING DELTA AS "
            "SELECT customer_key, wwi_customer_id, customer, valid_from, valid_to "
            f"FROM {source} ORDER BY customer_key")


def run(spark, contract, *, catalog, gold_schema, drop):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    entry = next(e for e in contract["entries"]
                 if e["table"] == "gold.report_customer_history" and e["drop"] == drop)
    if entry["columns"] != COLUMNS:
        raise ValueError("customer-history contract columns changed")
    spark.sql(report_sql(catalog, gold_schema)).collect()


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "drop")})
