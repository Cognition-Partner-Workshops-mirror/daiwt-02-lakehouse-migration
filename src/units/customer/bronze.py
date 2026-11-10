# Databricks notebook source
import json
from pathlib import Path

from customer_logic import TABLES, cutoffs, datetime7, table_name

drop = dbutils.widgets.get("drop")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("bronze_schema")
golden = dbutils.widgets.get("golden_path")
manifest = json.loads(Path(golden + "/manifest.json").read_text())
last, new = cutoffs(manifest, drop)
for key, derived in (("last_cutoff", last), ("new_cutoff", new)):
    supplied = dbutils.widgets.get(key)
    if supplied and datetime7(supplied) != derived:
        raise ValueError(f"{key} must match the manifest for {drop}")
    dbutils.jobs.taskValues.set(key=key, value=derived)

def read_csv(path, columns):
    return (spark.read.schema(", ".join(f"`{c}` STRING" for c in columns))
            .option("header", True).option("nullValue", "\\N")
            .option("quote", '"').option("escape", '"').option("multiLine", True)
            .option("mode", "FAILFAST").csv(golden + "/" + path))

for source in TABLES:
    state = None
    for index in range(int(drop[1:]) + 1):
        path = f"inputs/D{index}/WideWorldImporters.{source}.csv"
        spec = manifest["files"][path]
        columns = spec["columns"]
        incoming = read_csv(path, columns)
        kind = spec.get("input_kind", "full" if index == 0 else "upsert")
        if index == 0 or kind == "full":
            state = incoming
        elif kind == "upsert":
            keys = spec["key_columns"]
            deleted = path[:-4] + ".deleted.csv"
            if deleted in manifest["files"]:
                deletion = read_csv(deleted, manifest["files"][deleted]["columns"])
                if spec["primary_key"]:
                    predicate = [state[k].eqNullSafe(deletion[k]) for k in keys]
                    state = state.join(deletion, predicate, "left_anti")
                else:
                    state = state.exceptAll(deletion.select(*columns))
            if spec["primary_key"]:
                predicate = [state[k].eqNullSafe(incoming[k]) for k in keys]
                state = state.join(incoming, predicate, "left_anti").unionByName(incoming)
            else:
                state = state.unionByName(incoming)
        else:
            raise ValueError(f"Unsupported input_kind {kind}")
    target = f"{catalog}.{schema}.{table_name(source)}"
    state.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(target)
    print(f"BRONZE {target}: {state.count()} rows")

spark.createDataFrame([(drop, last, new)], "drop STRING, last_cutoff STRING, new_cutoff STRING").write.mode(
    "overwrite").saveAsTable(f"{catalog}.{schema}.customer_run_window")
