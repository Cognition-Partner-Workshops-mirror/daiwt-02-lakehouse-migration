# Databricks notebook source
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))
from s11_sale.runtime import ingest

for name, default in [("catalog", ""), ("bronze_schema", ""), ("golden_path", ""),
                      ("drop", "D0"), ("last_cutoff", ""), ("new_cutoff", "")]:
    dbutils.widgets.text(name, default)

ingest(spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("bronze_schema"),
       dbutils.widgets.get("golden_path"), dbutils.widgets.get("drop"),
       dbutils.widgets.get("last_cutoff"), dbutils.widgets.get("new_cutoff"))
