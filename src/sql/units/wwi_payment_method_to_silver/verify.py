# Databricks notebook source
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent) if '__file__' in globals() else
                '/Workspace' + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0])
from payment_method import assert_parity, expected

catalog = dbutils.widgets.get('catalog')
silver = dbutils.widgets.get('silver_schema')
drop = dbutils.widgets.get('drop')
root = dbutils.widgets.get('golden_path')
actual = [row.asDict() for row in spark.table(f'{catalog}.{silver}.payment_method_changes').collect()]
assert_parity(actual, expected(root, drop))
assert spark.table(f'{catalog}.{silver}.payment_method_changes_quarantine').count() == 0
print(f'PASS Payment Method {drop}: {len(actual)} rows; quarantine empty')
