"""Re-evaluate each extraction against its own immutable bronze drop history."""
import json
from pyspark import pipelines as dp
from transaction_s13 import EXPECTATIONS, extract_sql

with open(spark.conf.get('s13.golden_path') + '/manifest.json') as stream:
    drops = json.load(stream)['drops']
bronze = f"`{spark.conf.get('s13.catalog')}`.`{spark.conf.get('s13.bronze_schema')}`"


@dp.materialized_view(name='transaction_changes', table_properties={'delta.columnMapping.mode': 'name'})
@dp.expect_all(EXPECTATIONS)
def transaction_changes():
    return spark.sql(extract_sql(bronze, drops))


@dp.materialized_view(name='transaction_quarantine', table_properties={'delta.columnMapping.mode': 'name'})
def transaction_quarantine():
    failures = ' OR '.join(f'NOT coalesce(({rule}), false)' for name, rule in EXPECTATIONS.items() if name != 'totals')
    return spark.read.table('transaction_changes').where(failures)
