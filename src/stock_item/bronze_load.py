# Databricks notebook source
import json
from pathlib import Path

import model
from model import SOURCES, window
from pyspark.sql import Window
from pyspark.sql import functions as F

for name in ("catalog", "bronze_schema", "golden_path", "drop"):
    dbutils.widgets.text(name, "")
catalog, schema, golden, drop = [dbutils.widgets.get(n) for n in
                                ("catalog", "bronze_schema", "golden_path", "drop")]
if catalog not in ("daiwt_02_mig_dev", "daiwt_02_mig_prod"):
    raise ValueError("Unsupported S05 catalog")
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
    incoming = (incoming.withColumn("_drop", F.lit(drop)).withColumn("_file", F.lit(str(path)))
                .withColumn("_ingested_at", F.current_timestamp()))
    if drop == "D0":
        result = incoming
    else:
        existing = spark.table(target)
        deleted = path.with_name(path.stem + ".deleted.csv")
        if deleted.exists():
            removals = read_csv(deleted).select(*columns)
            if pk:
                existing = existing.join(removals.select(pk), pk, "left_anti")
            else:
                counts = removals.groupBy(*columns).count().withColumnRenamed("count", "_s05_deleted")
                ranked = existing.withColumn("_s05_occurrence", F.row_number().over(
                    Window.partitionBy(*columns).orderBy("_ingested_at", "_drop", "_file")))
                condition = [F.col(f"e.`{c}`").eqNullSafe(F.col(f"d.`{c}`")) for c in columns]
                existing = (ranked.alias("e").join(counts.alias("d"), condition, "left")
                            .where(F.col("e._s05_occurrence") > F.coalesce(F.col("d._s05_deleted"), F.lit(0)))
                            .select("e.*").drop("_s05_occurrence"))
        if pk:
            existing = existing.join(incoming.select(pk), pk, "left_anti")
        result = existing.unionByName(incoming)
    # Materialize before replacing a table used in the replay query.
    scratch = f"{bronze}._s05_{table}_replay"
    result.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(scratch)
    spark.sql(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {scratch}")
    spark.sql(f"DROP TABLE {scratch}")

capture_path = Path(model.__file__).resolve().parents[2] / "golden" / f"capture-{drop}.json"
capture = json.loads(capture_path.read_text())
last, new = window(capture, drop)
(spark.createDataFrame([(drop, last, new)], "drop STRING, last_cutoff STRING, new_cutoff STRING")
 .write.mode("overwrite").saveAsTable(f"{bronze}.stock_item_window"))
