# Databricks notebook source
import json
from transaction_s13 import SOURCES

catalog = dbutils.widgets.get('catalog')
bronze = f"`{catalog}`.`{dbutils.widgets.get('bronze_schema')}`"
golden = dbutils.widgets.get('golden_path').rstrip('/')
drop = dbutils.widgets.get('drop')
assert catalog == 'daiwt_02_mig_dev', 'S13 stand-in is dev-only'
with open(golden + '/manifest.json') as stream:
    manifest = json.load(stream)

for source, table in SOURCES.items():
    path = f'inputs/{drop}/WideWorldImporters.{source}.csv'
    spec = manifest['files'][path]
    columns = spec['columns']
    pk = spec['primary_key'][0]
    current, raw = f'{bronze}.{table}', f'{bronze}.{table}_raw'
    definition = ', '.join(f'`{c}` STRING' for c in columns)
    projection = ', '.join(f'`{c}`' for c in columns)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {current} ({definition}, _loaded_drop STRING) USING DELTA")
    loaded = spark.sql(f'SELECT DISTINCT _loaded_drop FROM {current}').collect()
    loaded = {r[0] for r in loaded}
    if loaded and max(loaded) >= drop:
        assert len(loaded) == 1, f'Mixed drop markers: {table}'
        loaded_path = f'inputs/{max(loaded)}/WideWorldImporters.{source}.csv'
        assert spark.table(current).count() == manifest['files'][loaded_path]['full_row_count']
        continue
    previous = spec['previous_input']
    assert loaded == ({previous.split('/')[1]} if previous else set()), f'Out of order: {table} {drop} {loaded}'
    spark.sql(f"CREATE TABLE IF NOT EXISTS {raw} ({definition}, _drop STRING, _op STRING, _source_file STRING, _row_ordinal BIGINT) USING DELTA")
    delete_path = path[:-4] + '.deleted.csv'
    for file, op in [(delete_path, 'delete'), (path, 'base' if drop == 'D0' else 'upsert')]:
        if file not in manifest['files']:
            continue
        file_columns = manifest['files'][file]['columns']
        schema = ', '.join(f'`{c}` STRING' for c in file_columns)
        selected = ', '.join(f'`{c}`' if c in file_columns else f'CAST(NULL AS STRING) AS `{c}`' for c in columns)
        landed = spark.sql(f"""SELECT {selected}, '{drop}' _drop, '{op}' _op,
            '{file}' _source_file, row_number() OVER (ORDER BY `{pk}`) _row_ordinal
            FROM read_files('{golden}/{file}', format => 'csv', header => true,
                schema => '{schema}', nullValue => '\\\\N', emptyValue => '',
                multiLine => true, escape => '"', mode => 'FAILFAST')""")
        landed.createOrReplaceTempView('s13_landed')
        spark.sql(f"""MERGE INTO {raw} t USING s13_landed s
            ON t._drop = s._drop AND t._op = s._op AND t._row_ordinal = s._row_ordinal
            WHEN NOT MATCHED THEN INSERT *""")
    marker = previous.split('/')[1] if previous else drop
    spark.sql(f"SELECT {projection}, '{marker}' _loaded_drop FROM {raw} WHERE _drop = '{drop}' AND _op <> 'delete'").createOrReplaceTempView('s13_upserts')
    if drop == 'D0':
        spark.sql(f'CREATE OR REPLACE TABLE {current} USING DELTA AS SELECT * FROM s13_upserts')
    else:
        missing = spark.sql(f"""SELECT `{pk}` FROM {raw} WHERE _drop = '{drop}' AND _op = 'delete'
            EXCEPT SELECT `{pk}` FROM (
                SELECT *, row_number() OVER (PARTITION BY `{pk}` ORDER BY _drop DESC,
                    CASE WHEN _op = 'upsert' THEN 1 ELSE 0 END DESC) rn
                FROM {raw} WHERE _drop < '{drop}') WHERE rn = 1 AND _op <> 'delete'""").count()
        assert missing == 0, 'Deleted key absent from snapshot'
        spark.sql(f"DELETE FROM {current} WHERE `{pk}` IN (SELECT `{pk}` FROM {raw} WHERE _drop = '{drop}' AND _op = 'delete')")
        spark.sql(f"MERGE INTO {current} t USING s13_upserts s ON t.`{pk}` = s.`{pk}` WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    assert spark.table(current).count() == spec['full_row_count'], f'Snapshot count: {table} {drop}'
    spark.sql(f"UPDATE {current} SET _loaded_drop = '{drop}'")
