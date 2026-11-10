# Databricks notebook source
import json

from transaction_type import window

for name, default in (("catalog", "daiwt_02_mig_dev"),
                      ("silver_schema", "silver_pr_8"), ("golden_path", "/Volumes/daiwt_02_mig_prod/landing/golden"),
                      ("drop", "D0"), ("last_cutoff", ""), ("new_cutoff", "")):
    dbutils.widgets.text(name, default)
catalog, silver, golden, drop, last_cutoff, new_cutoff = [dbutils.widgets.get(n) for n in
    ("catalog", "silver_schema", "golden_path", "drop", "last_cutoff", "new_cutoff")]
assert catalog in ("daiwt_02_mig_dev", "daiwt_02_mig_prod")
assert drop in ("D0", "D1", "D2")
manifest = json.loads(spark.read.text(f"{golden}/manifest.json", wholetext=True).first().value)
last, new = window(manifest, drop)
assert new_cutoff == new
assert drop == "D0" or last_cutoff == last
spark.createDataFrame([(drop, last, new)], "drop STRING, last_cutoff STRING, new_cutoff STRING").write.mode(
    "overwrite").saveAsTable(f"{catalog}.{silver}.s07_run_window")
print(f"S07 {drop}: catalog={catalog}, window=({last}, {new}]")
