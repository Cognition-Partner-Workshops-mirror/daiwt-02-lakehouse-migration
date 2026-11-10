# Databricks notebook source
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent) if '__file__' in globals() else
                '/Workspace' + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0])
from payment_method import window

catalog = dbutils.widgets.get('catalog')
silver = dbutils.widgets.get('silver_schema')
drop = dbutils.widgets.get('drop')
last_cutoff = dbutils.widgets.get('last_cutoff')
new_cutoff = dbutils.widgets.get('new_cutoff')
spark.createDataFrame([window(drop, last_cutoff, new_cutoff)],
                      'drop STRING, last_cutoff STRING, new_cutoff STRING').write.mode('overwrite').saveAsTable(
                          f'{catalog}.{silver}.payment_method_window')
