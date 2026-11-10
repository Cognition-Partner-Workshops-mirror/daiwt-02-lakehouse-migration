# Databricks notebook source
import json
from pathlib import Path

from model import SOURCES, window
from pyspark.sql import functions as F

for name in ("catalog", "bronze_schema", "golden_path", "drop"):
    dbutils.widgets.text(name, "")
catalog, schema, golden, drop = [dbutils.widgets.get(n) for n in
                                ("catalog", "bronze_schema", "golden_path", "drop")]
if catalog == "daiwt_02_mig_prod":
    raise ValueError("S05 replay must not write production")
if drop not in ("D0", "D1", "D2"):
    raise ValueError(drop)
bronze = f"`{catalog}`.`{schema}`"


def read_csv(path):
    return (spark.read.option("header", True).option("inferSchema", False)
            .option("nullValue", "\\N").option("escape", '"')
            .option("multiLine", True).csv(str(path)))


for source, (table, pk) in SOURCES.items():
    path = Path(golden) / "inputs" / drop / f"WideWorldImporters.Warehouse.{source}.csv"
    target = f"{bronze}.{table}"
    incoming = read_csv(path)
    columns = incoming.columns
    if drop == "D0":
        result = incoming
    else:
        existing = spark.table(target).select(*columns)
        deleted = path.with_name(path.stem + ".deleted.csv")
        if deleted.exists():
            removals = read_csv(deleted).select(*columns)
            existing = (existing.join(removals.select(pk), pk, "left_anti")
                        if pk else existing.exceptAll(removals))
        if pk:
            existing = existing.join(incoming.select(pk), pk, "left_anti")
        result = existing.unionByName(incoming)
    # Materialize before replacing a table used in the replay query.
    scratch = f"{bronze}._s05_{table}_replay"
    (result.withColumn("_drop", F.lit(drop)).withColumn("_file", F.lit(str(path)))
     .withColumn("_ingested_at", F.current_timestamp())
     .write.mode("overwrite").option("overwriteSchema", True).saveAsTable(scratch))
    spark.sql(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {scratch}")
    spark.sql(f"DROP TABLE {scratch}")

capture = json.loads((Path(golden) / f"capture-{drop}.json").read_text())
last, new = window(capture, drop)
(spark.createDataFrame([(drop, last, new)], "drop STRING, last_cutoff STRING, new_cutoff STRING")
 .write.mode("overwrite").saveAsTable(f"{bronze}.stock_item_window"))
