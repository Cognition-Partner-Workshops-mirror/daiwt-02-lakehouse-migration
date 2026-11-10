# Databricks notebook source
from pyspark.sql import functions as F

from stock_holding_common import PARITY, parity_price_type, qualified, verification_drops

for name, default in [("catalog", ""), ("bronze_schema", ""), ("silver_schema", ""),
                      ("drop", "D2"), ("golden_path", "/Volumes/daiwt_02_mig_prod/landing/golden")]:
    dbutils.widgets.text(name, default)
catalog, bronze, silver = [dbutils.widgets.get(n) for n in ("catalog", "bronze_schema", "silver_schema")]
replay_log = qualified(catalog, bronze, "bronze_replay_log")
snapshot = qualified(catalog, silver, "stock_holding_snapshot")
quarantine = qualified(catalog, silver, "stock_holding_snapshot_quarantine")
result_table = qualified(catalog, silver, "stock_holding_parity")
landed = sorted(r[0] for r in spark.table(replay_log).where(
    "source_table = 'Warehouse.StockItemHoldings' AND status = 'PASS'").select("drop").distinct().collect())
requested = verification_drops(landed, dbutils.widgets.get("drop"))


def read_golden(drop, table):
    path = f"{dbutils.widgets.get('golden_path').rstrip('/')}/outputs/{drop}/tables/{table}.csv"
    return spark.read.option("header", True).option("nullValue", "\\N").csv(path)


results = []
for drop in requested:
    fact = read_golden(drop, "Fact.Stock_Holding")
    dimension = read_golden(drop, "Dimension.Stock_Item").select("stock_item_key", "wwi_stock_item_id")
    duplicate_keys = dimension.groupBy("stock_item_key").count().where("count <> 1").count()
    mapped = fact.join(dimension, "stock_item_key", "left")
    unmapped = mapped.where("wwi_stock_item_id IS NULL").count()
    expected = mapped.select(*PARITY)
    actual = spark.table(snapshot).where(F.col("drop") == drop)
    actual = actual.withColumn("last_cost_price", F.col("last_cost_price")
                              .cast(parity_price_type(drop)).cast("decimal(18,2)"))
    actual = actual.select(*(F.col(c).cast("string").alias(c) for c in PARITY))
    actual_count, expected_count = actual.count(), expected.count()
    actual_only = actual.exceptAll(expected).count()
    golden_only = expected.exceptAll(actual).count()
    quarantined = spark.table(quarantine).where(F.col("drop") == drop).count()
    ok = (actual_count == expected_count == 227 and actual_only == golden_only ==
          quarantined == unmapped == duplicate_keys == 0)
    results.append((drop, actual_count, expected_count, actual_only, golden_only,
                    unmapped, duplicate_keys, quarantined, "PASS" if ok else "FAIL"))

result = spark.createDataFrame(results, "drop string, actual_rows long, golden_rows long, actual_only long, golden_only long, unmapped_keys long, duplicate_dimension_keys long, quarantine_rows long, status string")
result.write.mode("overwrite").saveAsTable(result_table)
result.orderBy("drop").show(truncate=False)
if any(r[-1] != "PASS" for r in results):
    raise AssertionError("Stock holding parity failed; see stock_holding_parity")
