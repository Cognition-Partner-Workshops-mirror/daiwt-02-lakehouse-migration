# Databricks notebook source
import json
import re

from pyspark.sql import functions as F


catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
golden_path = dbutils.widgets.get("golden_path").rstrip("/")
drop = dbutils.widgets.get("drop")
if drop not in ("D0", "D1", "D2"):
    raise ValueError("Invalid drop")
if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v) for v in (catalog, bronze_schema)):
    raise ValueError("Invalid catalog/schema")
namespace = f"`{catalog}`.`{bronze_schema}`"
control = f"{namespace}.employee_drop_window"
manifest = json.loads(spark.read.option("wholetext", True).text(golden_path + "/manifest.json").first()[0])


def read_csv(path, columns):
    schema = ", ".join(f"`{c}` STRING" for c in columns)
    return spark.sql(
        "SELECT * FROM read_files(:path, format => 'csv', header => true, "
        "schema => :schema, nullValue => :null_value, emptyValue => '', "
        "multiLine => true, escape => '\"', mode => 'FAILFAST')",
        args={"path": golden_path + "/" + path, "schema": schema, "null_value": "\\N"},
    )


if drop != "D0":
    previous = "D" + str(int(drop[1:]) - 1)
    loaded = spark.table(control).select("_loaded_drop").collect()
    if len(loaded) != 1 or loaded[0][0] != previous:
        raise ValueError("Employee drops must be applied exactly once and in order")

for source, name in (("People", "application_people"), ("People_Archive", "application_people_archive")):
    path = f"inputs/{drop}/WideWorldImporters.Application.{source}.csv"
    spec = manifest["files"][path]
    columns = spec["columns"]
    target = f"{namespace}.{name}"
    raw = f"{namespace}.{name}_raw"
    upserts = read_csv(path, columns)
    deleted_path = path.removesuffix(".csv") + ".deleted.csv"
    deleted = None
    if deleted_path in manifest["files"]:
        deleted = read_csv(deleted_path, manifest["files"][deleted_path]["columns"])
    for frame, op, file in ((upserts, "base" if drop == "D0" else "upsert", path),
                            (deleted, "delete", deleted_path)):
        if frame is not None:
            frame = frame.select(*(F.col(c) if c in frame.columns else F.lit(None).cast("string").alias(c)
                                   for c in columns))
            (frame.withColumn("_drop", F.lit(drop)).withColumn("_op", F.lit(op))
             .withColumn("_source_file", F.lit(file)).withColumn("_ingested_at", F.current_timestamp())
             .write.mode("append").saveAsTable(raw))
    if drop == "D0":
        upserts.withColumn("_loaded_drop", F.lit(drop)).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    elif source == "People":
        if deleted is not None:
            if deleted.join(spark.table(target), "PersonID", "left_anti").count():
                raise ValueError("Deleted People key is absent")
            deleted.createOrReplaceTempView("employee_deleted")
            spark.sql(f"DELETE FROM {target} WHERE PersonID IN (SELECT PersonID FROM employee_deleted)")
        upserts.withColumn("_loaded_drop", F.lit(drop)).createOrReplaceTempView("employee_upserts")
        spark.sql(f"MERGE INTO {target} t USING employee_upserts s ON t.PersonID = s.PersonID "
                  "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
        spark.sql(f"UPDATE {target} SET _loaded_drop = '{drop}'")
    else:
        current = spark.table(target).select(*columns)
        if deleted is not None:
            if deleted.exceptAll(current).count():
                raise ValueError("Deleted archive row is absent")
            current = current.exceptAll(deleted)
        # Materialize independently before overwriting a table that supplies the input.
        staged = f"{namespace}.employee_archive_replay_stage"
        current.unionByName(upserts).withColumn("_loaded_drop", F.lit(drop)).write.mode("overwrite").saveAsTable(staged)
        spark.sql(f"INSERT OVERWRITE TABLE {target} SELECT * FROM {staged}")
        spark.sql(f"DROP TABLE {staged}")
    spark.sql(f"SELECT CASE WHEN COUNT(*) = {int(spec['full_row_count'])} THEN 'ok' "
              f"ELSE raise_error('Employee bronze snapshot count mismatch: {source} {drop}') END FROM {target}").collect()

window = next(d for d in manifest["drops"] if d["name"] == drop)


def cutoff(value):
    if isinstance(value, list):
        value = next(r["Cutoff Time"] for r in value if r["Table Name"] == "Employee")
    return value if "." in value else value + ".0000000"


spark.createDataFrame([(drop, cutoff(window["last_cutoff"]), cutoff(window["new_cutoff"]))],
                      "_loaded_drop STRING, last_cutoff STRING, new_cutoff STRING").write.mode("overwrite").saveAsTable(control)
