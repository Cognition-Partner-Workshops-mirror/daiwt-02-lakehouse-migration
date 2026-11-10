# Databricks notebook source
for name in ('catalog', 'silver_schema', 'drop', 'last_cutoff', 'new_cutoff'):
    dbutils.widgets.text(name, '')
catalog, schema, drop, last, new = [dbutils.widgets.get(name) for name in
                                  ('catalog', 'silver_schema', 'drop', 'last_cutoff', 'new_cutoff')]
if drop not in ('D0', 'D1', 'D2'):
    raise ValueError('invalid drop')
spark.createDataFrame([(drop, last, new)], 'drop STRING, last_cutoff STRING, new_cutoff STRING').write.mode(
    'overwrite').saveAsTable(f'{catalog}.{schema}.s06_supplier_run_window')
