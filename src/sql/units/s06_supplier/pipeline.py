from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from logic import change_sql

spark = SparkSession.builder.getOrCreate()
bronze = spark.conf.get('s06.bronze')
last = spark.conf.get('last_cutoff')
new = spark.conf.get('new_cutoff')
drop = spark.conf.get('drop')
if drop not in ('D0', 'D1', 'D2'):
    raise ValueError('invalid drop')

VALID = ('wwi_supplier_id IS NOT NULL AND supplier IS NOT NULL AND category IS NOT NULL '
         'AND primary_contact IS NOT NULL AND valid_from < valid_to '
         'AND try_cast(wwi_supplier_id AS INT) > 0 '
         'AND try_cast(payment_days AS INT) >= 0')


@dp.materialized_view(name='supplier_changes')
@dp.expect('supplier_values', VALID)
def supplier_changes():
    return spark.sql(change_sql(bronze, last, new)).replace('\\N', None)


@dp.materialized_view(name='supplier_changes_quarantine')
def supplier_changes_quarantine():
    return spark.table('supplier_changes').where(f'NOT coalesce(({VALID}), false)')
