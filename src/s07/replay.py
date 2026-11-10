# Databricks notebook source
import json

from pyspark.sql import functions as F

from transaction_type import SOURCE_COLUMNS, replay, window

for name, default in (("catalog", "daiwt_02_mig_dev"), ("bronze_schema", "bronze_pr_8"),
                      ("silver_schema", "silver_pr_8"), ("golden_path", "/Volumes/daiwt_02_mig_prod/landing/golden"),
                      ("drop", "D0")):
    dbutils.widgets.text(name, default)
catalog, bronze, silver, golden, drop = [dbutils.widgets.get(n) for n in
    ("catalog", "bronze_schema", "silver_schema", "golden_path", "drop")]
assert catalog in ("daiwt_02_mig_dev", "daiwt_02_mig_prod")
assert drop in ("D0", "D1", "D2")
manifest = json.loads(spark.read.text(f"{golden}/manifest.json", wholetext=True).first().value)


def read_csv(path):
    return spark.read.options(header=True, inferSchema=False, nullValue="\\N", mode="FAILFAST").csv(path)


for source, target, archive in (("TransactionTypes", "application_transaction_types", False),
                                ("TransactionTypes_Archive", "application_transaction_types_archive", True)):
    state = []
    for index in range(int(drop[1:]) + 1):
        replay_drop = f"D{index}"
        path = f"inputs/{replay_drop}/WideWorldImporters.Application.{source}.csv"
        frame = read_csv(f"{golden}/{path}")
        upserts = [r.asDict() for r in frame.collect()]
        deleted_path = path.removesuffix(".csv") + ".deleted.csv"
        deletes = []
        if deleted_path in manifest["files"]:
            deletes = [r.asDict() for r in read_csv(f"{golden}/{deleted_path}").collect()]
        state = replay(state, upserts, deletes, archive)
        if replay_drop == drop:
            (frame.withColumn("_drop", F.lit(drop)).withColumn("_file", F.lit(path))
             .withColumn("_ingested_at", F.current_timestamp()).withColumn("_operation", F.lit("upsert"))
             .write.mode("append").saveAsTable(f"{catalog}.{bronze}.{target}_raw"))
            if deletes:
                (read_csv(f"{golden}/{deleted_path}").select(*[
                    F.col(c) if c in deletes[0] else F.lit(None).cast("string").alias(c) for c in SOURCE_COLUMNS])
                 .withColumn("_drop", F.lit(drop)).withColumn("_file", F.lit(deleted_path))
                 .withColumn("_ingested_at", F.current_timestamp()).withColumn("_operation", F.lit("delete"))
                 .write.mode("append").saveAsTable(f"{catalog}.{bronze}.{target}_raw"))
    typed = spark.createDataFrame([tuple(r[c] for c in SOURCE_COLUMNS) for r in state],
                                 ", ".join(f"{c} STRING" for c in SOURCE_COLUMNS))
    (typed.withColumn("TransactionTypeID", F.col("TransactionTypeID").cast("int"))
     .withColumn("LastEditedBy", F.col("LastEditedBy").cast("int"))
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{bronze}.{target}"))

spark.sql(f"""CREATE OR REPLACE VIEW {catalog}.{silver}.v_transaction_types_asof AS
    SELECT * FROM {catalog}.{bronze}.application_transaction_types_archive
    UNION ALL SELECT * FROM {catalog}.{bronze}.application_transaction_types""")
last, new = window(manifest, drop)
spark.createDataFrame([(drop, last, new)], "drop STRING, last_cutoff STRING, new_cutoff STRING").write.mode(
    "overwrite").saveAsTable(f"{catalog}.{silver}.s07_run_window")
print(f"S07 bronze {drop}: catalog={catalog}, window=({last}, {new}]")
