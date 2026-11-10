# Databricks notebook source
import csv
from datetime import datetime, timezone
from pathlib import Path

from logic import SOURCES, read_csv, replay

for name in ('catalog', 'bronze_schema', 'golden_path', 'drop', 'last_cutoff', 'new_cutoff'):
    dbutils.widgets.text(name, '')
catalog, schema, root, drop = [dbutils.widgets.get(n) for n in
                              ('catalog', 'bronze_schema', 'golden_path', 'drop')]
if drop not in ('D0', 'D1', 'D2'):
    raise ValueError('invalid drop')

# P4 stand-in: reconstruct the selected snapshot from immutable ordered drops.
for source, (table, key) in SOURCES.items():
    state, raw = [], []
    columns = None
    for index in range(int(drop[1:]) + 1):
        current_drop = f'D{index}'
        path = Path(root) / 'inputs' / current_drop / f'WideWorldImporters.{source}.csv'
        with path.open(newline='', encoding='utf-8-sig') as stream:
            columns = next(csv.reader(stream))
        additions = read_csv(path)
        delete_path = path.with_suffix('.deleted.csv')
        deleted = read_csv(delete_path) if delete_path.exists() else []
        now = datetime.now(timezone.utc).isoformat()
        for operation, rows, file in [('delete', deleted, delete_path), ('upsert', additions, path)]:
            for row in rows:
                raw.append(tuple(row[c] for c in columns) +
                           (current_drop, str(file), now, operation))
        # Replay only source columns so archive identity excludes ingest metadata.
        previous = {tuple(r[c] for c in columns): r for r in state}
        values = replay([{c: r[c] for c in columns} for r in state], additions, deleted, key)
        incoming = {tuple(r[c] for c in columns) for r in additions}
        state = []
        for row in values:
            signature = tuple(row[c] for c in columns)
            metadata = (current_drop, str(path), now) if signature in incoming else tuple(
                previous[signature][c] for c in ('_drop', '_file', '_ingested_at'))
            state.append(dict(row, **dict(zip(('_drop', '_file', '_ingested_at'), metadata))))
    fields = columns + ['_drop', '_file', '_ingested_at']
    ddl = ', '.join(f'`{c}` STRING' for c in fields)
    spark.createDataFrame([tuple(r[c] for c in fields) for r in state], ddl).write.mode(
        'overwrite').option('overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.{table}')
    spark.createDataFrame(raw, ddl + ', `_operation` STRING').write.mode('overwrite').option(
        'overwriteSchema', 'true').saveAsTable(f'{catalog}.{schema}.s06_supplier_raw_{table}')

spark.createDataFrame([(drop, dbutils.widgets.get('last_cutoff'), dbutils.widgets.get('new_cutoff'))],
                      'drop STRING, last_cutoff STRING, new_cutoff STRING').write.mode(
    'overwrite').saveAsTable(f'{catalog}.{schema}.s06_supplier_run_window')
