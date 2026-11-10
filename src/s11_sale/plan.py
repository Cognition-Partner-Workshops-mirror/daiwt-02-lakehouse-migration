# Databricks notebook source
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parent))
from s11_sale.runtime import manifest_at
from s11_sale.sale import drop_window

for name, default in [("golden_path", ""), ("drop", "all"), ("last_cutoff", ""), ("new_cutoff", "")]:
    dbutils.widgets.text(name, default)

manifest = manifest_at(dbutils.widgets.get("golden_path"))
drop = dbutils.widgets.get("drop")
last, new = dbutils.widgets.get("last_cutoff"), dbutils.widgets.get("new_cutoff")
if drop == "all" and (last or new):
    raise ValueError("Explicit cutoffs require one drop, not all")
for name in ["D0", "D1", "D2"]:
    start, end = drop_window(manifest, name)
    dbutils.jobs.taskValues.set(key=f"{name.lower()}_last", value=start)
    dbutils.jobs.taskValues.set(key=f"{name.lower()}_new", value=end)
start, end = drop_window(manifest, "D0" if drop == "all" else drop, last, new)
dbutils.jobs.taskValues.set(key="single_last", value=start)
dbutils.jobs.taskValues.set(key="single_new", value=end)
