# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent) if '__file__' in globals() else
                '/Workspace' + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0])
from transaction_s13 import GOLD, NAMES, cutoff

catalog = dbutils.widgets.get('catalog')
silver = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`"
golden = dbutils.widgets.get('golden_path').rstrip('/')
drop = dbutils.widgets.get('drop')
sizes = {'D0': 99585, 'D1': 705, 'D2': 842}
if drop not in sizes:
    raise ValueError(f'Unknown drop: {drop}')
with open(golden + '/manifest.json') as stream:
    metadata = next(d for d in json.load(stream)['drops'] if d['name'] == drop)
for parameter in ('last_cutoff', 'new_cutoff'):
    if cutoff(dbutils.widgets.get(parameter)) != cutoff(metadata[parameter]):
        raise ValueError(f'Unexpected {parameter} for {drop}')


def golden_rows(name):
    return spark.sql(f"""SELECT * FROM read_files(
        '{golden}/outputs/{name}/tables/Fact.Transaction.csv', format => 'csv',
        header => true, inferSchema => false, nullValue => '\\\\N', emptyValue => '',
        multiLine => true, escape => '"', mode => 'FAILFAST')""")


full = golden_rows(drop)
gained = full if drop == 'D0' else full.exceptAll(golden_rows(f'D{int(drop[1:]) - 1}'))
gained.select(*GOLD).createOrReplaceTempView('s13_expected')
projection = ', '.join(f'CAST(`{n}` AS STRING) AS `{g}`' for n, g in zip(NAMES[:11], GOLD))
spark.sql(f"SELECT {projection} FROM {silver}.transaction_changes WHERE _drop = '{drop}'").createOrReplaceTempView('s13_actual')
metrics = spark.sql(f"""SELECT
    '{drop}' AS drop,
    (SELECT count(*) FROM s13_expected) expected_count,
    (SELECT count(*) FROM s13_actual) actual_count,
    (SELECT count(*) FROM (SELECT * FROM s13_actual EXCEPT ALL SELECT * FROM s13_expected)) actual_minus_expected,
    (SELECT count(*) FROM (SELECT * FROM s13_expected EXCEPT ALL SELECT * FROM s13_actual)) expected_minus_actual,
    (SELECT count(*) FROM {silver}.transaction_quarantine) quarantine_count""")
metrics.createOrReplaceTempView('s13_metrics')
spark.sql(f"""SELECT CASE WHEN expected_count = {sizes[drop]} AND actual_count = {sizes[drop]}
    AND actual_minus_expected = 0 AND expected_minus_actual = 0 AND quarantine_count = 0
    THEN 'PASS {drop}' ELSE raise_error('S13 {drop} transaction multiset or quarantine mismatch') END
    FROM s13_metrics""").show()
result = metrics.first().asDict()
print(json.dumps(result))
dbutils.notebook.exit(json.dumps(result))
