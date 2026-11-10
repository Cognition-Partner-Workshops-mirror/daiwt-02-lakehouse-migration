# Databricks notebook source
import re
from functools import reduce

from pyspark.sql import functions as F
from logic import SOURCE_TYPES, SHARED, EXPECTED, has_deleted_file

catalog = dbutils.widgets.get("catalog")
bronze = dbutils.widgets.get("bronze_schema")
silver = dbutils.widgets.get("silver_schema")
drop = dbutils.widgets.get("drop")
last = dbutils.widgets.get("last_cutoff")
new = dbutils.widgets.get("new_cutoff")
mode = dbutils.widgets.get("mode")
root = dbutils.widgets.get("golden_path").rstrip("/")
for name in (catalog, bronze, silver):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError("Invalid identifier")
if drop not in EXPECTED:
    raise ValueError("Invalid drop")
table = f"{catalog}.{bronze}.warehouse_stock_item_transactions"
control = f"{catalog}.{silver}.movement_window"


def csv(path):
    return (spark.read.option("header", True).option("inferSchema", False)
            .option("nullValue", "\\N").option("mode", "FAILFAST").csv(path))


def ingest():
    path = f"{root}/inputs/{drop}/WideWorldImporters.Warehouse.StockItemTransactions.csv"
    raw = csv(path)
    def land(frame, file):
        return (frame.withColumn("_drop", F.lit(drop)).withColumn("_file", F.lit(file))
                .withColumn("_ingested_at", F.current_timestamp()))
    landed = land(raw, path)
    landed.write.mode("append").saveAsTable(table + "_raw")
    bad = reduce(lambda a, b: a | b, [
        F.col(c).isNotNull() & F.expr(f"try_cast(`{c}` AS {t})").isNull()
        for c, t in SOURCE_TYPES.items()])
    bad = bad | (F.col("Quantity").isNotNull() & F.expr("try_cast(try_cast(Quantity AS DECIMAL(18,3)) AS INT)").isNull())
    typed = landed.withColumn("_bad_cast", bad).selectExpr(
        *[f"try_cast(`{c}` AS {t}) AS `{c}`" for c, t in SOURCE_TYPES.items()],
        "_drop", "_file", "_ingested_at", "_bad_cast")
    if drop == "D0":
        typed.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(table)
    else:
        deleted = path.removesuffix(".csv") + ".deleted.csv"
        if has_deleted_file(deleted, dbutils.fs.ls(f"{root}/inputs/{drop}")):
            removed = csv(deleted)
            deleted_raw = removed
            for c in SOURCE_TYPES:
                if c not in deleted_raw.columns:
                    deleted_raw = deleted_raw.withColumn(c, F.lit(None).cast("string"))
            land(deleted_raw, deleted).select(*landed.columns).write.mode("append").saveAsTable(table + "_raw")
            removed.selectExpr("try_cast(StockItemTransactionID AS INT) AS id").createOrReplaceTempView("movement_deleted")
            spark.sql(f"DELETE FROM {table} WHERE StockItemTransactionID IN (SELECT id FROM movement_deleted)")
        typed.createOrReplaceTempView("movement_upserts")
        spark.sql(f"""MERGE INTO {table} t USING movement_upserts s
          ON t.StockItemTransactionID = s.StockItemTransactionID
          WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")


if mode == "ingest":
    ingest()
elif mode == "window":
    window = spark.createDataFrame([(drop, last, new)], "_drop STRING, last_cutoff STRING, new_cutoff STRING")
    window = window.selectExpr("_drop", "try_cast(last_cutoff AS TIMESTAMP_NTZ) AS last_cutoff",
                               "try_cast(new_cutoff AS TIMESTAMP_NTZ) AS new_cutoff")
    if window.where("last_cutoff IS NULL OR new_cutoff IS NULL OR last_cutoff > new_cutoff").count():
        raise ValueError("Invalid cutoff window")
    window.write.mode("overwrite").saveAsTable(control)
elif mode == "parity":
    def golden(d):
        frame = csv(f"{root}/outputs/{d}/tables/Fact.Movement.csv")
        return frame.selectExpr(*[f"CAST({c} AS {'DATE' if c == 'date_key' else 'INT'}) AS {c}" for c in SHARED])
    current = golden(drop)
    expected = current if drop == "D0" else current.exceptAll(golden("D" + str(int(drop[1:]) - 1)))
    staged = spark.table(f"{catalog}.{silver}.movement_changes")
    if staged.where(F.col("_drop") != drop).count():
        raise AssertionError("Stale Movement window")
    actual = staged.select(*SHARED)
    quarantine = spark.table(f"{catalog}.{silver}.movement_changes_quarantine").count()
    count = actual.count()
    missing, extra = expected.exceptAll(actual).count(), actual.exceptAll(expected).count()
    if quarantine or count != EXPECTED[drop] or missing or extra:
        raise AssertionError(f"Movement {drop}: rows={count}, missing={missing}, extra={extra}, quarantine={quarantine}")
    print(f"Movement {drop}: PASS rows={count}, missing=0, extra=0, quarantine=0")
else:
    raise ValueError("Invalid task mode")
