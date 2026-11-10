# Databricks notebook source
from collections import Counter
from logic import COLUMNS, expected

for name in ('catalog', 'silver_schema', 'golden_path', 'drop'):
    dbutils.widgets.text(name, '')
catalog, schema, root, drop = [dbutils.widgets.get(n) for n in
                              ('catalog', 'silver_schema', 'golden_path', 'drop')]
actual = Counter(tuple('\\N' if r[c] is None else str(r[c]) for c in COLUMNS)
                 for r in spark.table(f'{catalog}.{schema}.supplier_changes').collect())
wanted = expected(root, drop)
quarantine = spark.table(f'{catalog}.{schema}.supplier_changes_quarantine').count()
if actual != wanted or quarantine:
    print(f'S06 {drop}: missing={wanted - actual}; extra={actual - wanted}; quarantine={quarantine}')
    spark.sql("SELECT raise_error('S06 supplier parity mismatch or nonempty quarantine')").collect()
print(f'S06 {drop}: PASS rows={sum(actual.values())}; quarantine={quarantine}')
