# Databricks notebook source
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))
from wwi_sale_to_silver.runtime import manifest_at, validate_bronze
from wwi_sale_to_silver.sale import drop_window, target_namespace

for name in ("catalog", "bronze_schema", "golden_path", "drop", "last_cutoff", "new_cutoff"):
    dbutils.widgets.text(name, "")

manifest = manifest_at(dbutils.widgets.get("golden_path"))
drop = dbutils.widgets.get("drop")
last, new = drop_window(manifest, drop, dbutils.widgets.get("last_cutoff"),
                        dbutils.widgets.get("new_cutoff"))
bronze = target_namespace(dbutils.widgets.get("catalog"), dbutils.widgets.get("bronze_schema"), "bronze")
validate_bronze(spark, bronze, manifest, drop, last, new)
