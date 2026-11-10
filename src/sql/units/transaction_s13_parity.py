# Databricks notebook source
from transaction_s13 import GOLD, NAMES

catalog = dbutils.widgets.get('catalog')
silver = f"`{catalog}`.`{dbutils.widgets.get('silver_schema')}`"
golden = dbutils.widgets.get('golden_path').rstrip('/')


def golden_rows(drop):
    return spark.sql(f"""SELECT * FROM read_files(
        '{golden}/outputs/{drop}/tables/Fact.Transaction.csv', format => 'csv',
        header => true, inferSchema => false, nullValue => '\\\\N', emptyValue => '',
        multiLine => true, escape => '"', mode => 'FAILFAST')""")


previous = None
for drop, size in [('D0', 99585), ('D1', 705), ('D2', 842)]:
    full = golden_rows(drop)
    gained = full if previous is None else full.exceptAll(previous)
    expected = gained.select(*GOLD)
    expected.createOrReplaceTempView('s13_expected')
    projection = ', '.join(f'CAST(`{n}` AS STRING) AS `{g}`' for n, g in zip(NAMES[:11], GOLD))
    spark.sql(f"SELECT {projection} FROM {silver}.transaction_changes WHERE _drop = '{drop}'").createOrReplaceTempView('s13_actual')
    spark.sql(f"""SELECT CASE WHEN
        (SELECT count(*) FROM s13_expected) = {size} AND
        (SELECT count(*) FROM s13_actual) = {size} AND
        (SELECT count(*) FROM (SELECT * FROM s13_actual EXCEPT ALL SELECT * FROM s13_expected)) = 0 AND
        (SELECT count(*) FROM (SELECT * FROM s13_expected EXCEPT ALL SELECT * FROM s13_actual)) = 0
        THEN 'PASS {drop}' ELSE raise_error('S13 {drop} transaction multiset mismatch') END""").show()
    previous = full
spark.sql(f"SELECT CASE WHEN count(*) = 0 THEN 'PASS quarantine empty' ELSE raise_error('S13 quarantine is not empty') END FROM {silver}.transaction_quarantine").show()
