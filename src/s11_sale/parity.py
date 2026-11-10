# Databricks notebook source
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))
from s11_sale.runtime import check

for name, default in [("catalog", ""), ("bronze_schema", ""), ("silver_schema", ""),
                      ("golden_path", ""), ("sale_drop", "D0"), ("sale_last_cutoff", ""), ("sale_new_cutoff", "")]:
    dbutils.widgets.text(name, default)

result = check(spark, dbutils.widgets.get("catalog"), dbutils.widgets.get("bronze_schema"),
               dbutils.widgets.get("silver_schema"), dbutils.widgets.get("golden_path"),
               dbutils.widgets.get("sale_drop"), dbutils.widgets.get("sale_last_cutoff"),
               dbutils.widgets.get("sale_new_cutoff"))
dbutils.notebook.exit(__import__("json").dumps(result))
