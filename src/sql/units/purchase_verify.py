# Databricks notebook source
from purchase_core import PROJECT, SOURCES, input_path, manifest
from purchase_history import snapshot_sql
from pyspark.sql import functions as F

for name in ('catalog', 'bronze_schema', 'silver_schema', 'golden_path', 'drop'):
    dbutils.widgets.text(name, '')
catalog, bronze, silver, root = [dbutils.widgets.get(n) for n in
                                ('catalog', 'bronze_schema', 'silver_schema', 'golden_path')]
spec = manifest(root)
current_drop = dbutils.widgets.get('drop')
if current_drop not in ('D0', 'D1', 'D2'):
    raise ValueError('Unsupported purchase verification drop')
drops = ('D0', 'D1', 'D2')[:int(current_drop[1:]) + 1]
results = []


def record(drop, check, expected, actual):
    results.append((drop, check, expected, actual, 'PASS' if expected == actual else 'FAIL'))


def compare(drop, check, expected, actual):
    expected.createOrReplaceTempView('purchase_expected')
    actual.createOrReplaceTempView('purchase_actual')
    record(drop, check + '_count', expected.count(), actual.count())
    for left, right in (('expected', 'actual'), ('actual', 'expected')):
        difference = spark.sql(f'SELECT * FROM purchase_{left} EXCEPT ALL SELECT * FROM purchase_{right}')
        record(drop, check + '_' + left + '_minus_' + right, 0, difference.count())


def golden(drop):
    return (spark.read.option('header', True).option('inferSchema', False)
            .option('nullValue', '\\N').option('emptyValue', '').option('multiLine', True)
            .option('escape', '"').csv(f'{root}/outputs/{drop}/tables/Fact.Purchase.csv'))


changes = spark.table(f'{catalog}.{silver}.purchase_changes')
staging = spark.table(f'{catalog}.{silver}.purchase_staging')
record('D0', 'no_extract', 0, changes.where("_drop = 'D0'").count())
record('D0', 'no_staging', 0, staging.where("_staging_drop = 'D0'").count())
record('D0', 'quarantine_empty', 0, spark.table(f'{catalog}.{silver}.purchase_changes_quarantine').count())
record(current_drop, 'no_future_extract', 0, changes.where(F.col('_drop') > current_drop).count())
record(current_drop, 'no_future_staging', 0, staging.where(F.col('_staging_drop') > current_drop).count())
for drop in drops:
    log = spark.table(f'{catalog}.{bronze}.bronze_replay_log')
    record(drop, 'p4_replay_complete', 38, log.where(
        (F.col('drop') == drop) & (F.col('status') == 'PASS')).select('bronze_table').distinct().count())
    for source, table in SOURCES.items():
        record(drop, 'bronze_' + table, spec['files'][input_path(drop, source)]['full_row_count'],
               spark.sql(snapshot_sql(catalog, bronze, source, drop, spec)).count())
for n in range(1, int(current_drop[1:]) + 1):
    drop = f'D{n}'
    previous, current = golden(f'D{n-1}'), golden(drop)
    staged = staging.where(F.col('_staging_drop') == drop)
    inserted = current.join(previous.select('lineage_key').distinct(), 'lineage_key', 'left_anti')
    projection = [F.col(c).cast('string').alias(c) for c in PROJECT]
    compare(drop, 'inserted_staging', inserted.select(*projection), staged.select(*projection))
    record(drop, 'staging_size', {1: 57, 2: 113}[n], staged.count())
    parents = staged.select(F.col('wwi_purchase_order_id').cast('string').alias('wwi_purchase_order_id')).distinct()
    unchanged = previous.join(parents, 'wwi_purchase_order_id', 'left_anti').select(*previous.columns)
    compare(drop, 'unchanged_parents', unchanged,
            current.join(previous.select('purchase_key'), 'purchase_key', 'left_semi').select(*previous.columns))
    replaced = previous.join(parents, 'wwi_purchase_order_id', 'left_semi')
    record(drop, 'replaced_rows_gone', 0,
           current.join(replaced.select('purchase_key'), 'purchase_key', 'left_semi').count())

result = spark.createDataFrame(results, '`drop` STRING, `check` STRING, expected LONG, actual LONG, status STRING')
result.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(
    f'{catalog}.{silver}.purchase_parity_results')
result.show(len(results), truncate=False)
spark.sql(f"""SELECT CASE WHEN count(*) = 0 THEN 'PASS'
    ELSE raise_error('S10 purchase parity mismatch; see purchase_parity_results') END
    FROM {catalog}.{silver}.purchase_parity_results WHERE status <> 'PASS'""").collect()
