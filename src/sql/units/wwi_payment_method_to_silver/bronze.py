# Databricks notebook source
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent) if '__file__' in globals() else
                '/Workspace' + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0])
from payment_method import DROPS, SOURCE_COLUMNS, replay
from pyspark.sql import functions as F

catalog = dbutils.widgets.get('catalog')
bronze = dbutils.widgets.get('bronze_schema')
drop = dbutils.widgets.get('drop')
root = dbutils.widgets.get('golden_path')
last_cutoff = dbutils.widgets.get('last_cutoff')
new_cutoff = dbutils.widgets.get('new_cutoff')
if drop not in DROPS or last_cutoff >= new_cutoff:
    raise ValueError('Invalid drop or cutoff window')

for archive, table in [(False, 'application_payment_methods'), (True, 'application_payment_methods_archive')]:
    rows, audit = replay(root, drop, archive)
    schema = ', '.join(f'{c} STRING' for c in SOURCE_COLUMNS)
    raw_schema = schema + ', _drop STRING, _op STRING, _source_file STRING, _row_ordinal STRING'
    raw_name = f'{catalog}.{bronze}.{table}_raw'
    audit_frame = spark.createDataFrame(audit, raw_schema)
    if spark.catalog.tableExists(raw_name):
        loaded = spark.table(raw_name).select('_drop').distinct()
        audit_frame = audit_frame.join(loaded, '_drop', 'left_anti')
    audit_frame.write.mode('append').saveAsTable(raw_name)
    (spark.createDataFrame(rows, schema)
     .withColumn('PaymentMethodID', F.expr('try_cast(PaymentMethodID AS INT)'))
     .withColumn('LastEditedBy', F.expr('try_cast(LastEditedBy AS INT)'))
     .withColumn('_loaded_drop', F.lit(drop))
     .write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{bronze}.{table}'))

spark.createDataFrame([(drop, last_cutoff, new_cutoff)], 'drop STRING, last_cutoff STRING, new_cutoff STRING').write.mode('overwrite').saveAsTable(f'{catalog}.{bronze}.payment_method_window')
