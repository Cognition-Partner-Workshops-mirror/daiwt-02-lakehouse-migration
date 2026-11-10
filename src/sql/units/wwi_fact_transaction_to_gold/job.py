# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import identifier, relation, lookup_dim_key, fmt_datetime7
from sql.units.p5_gold_seed.integration import Integration, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import column_type, seed
from sql.units.wwi_transaction_to_silver.transaction_s13 import NAMES


def staged_order():
    return ("CASE WHEN _source_kind = 'customer' THEN 0 ELSE 1 END, "
            "last_modified_when, coalesce(wwi_customer_transaction_id, wwi_supplier_transaction_id)")


def stage_select(source, drop):
    columns = []
    for name in NAMES:
        column = name.lower().replace(' ', '_')
        value = f"`{name}`"
        if column == 'last_modified_when':
            value = fmt_datetime7(value, source='timestamp')
        columns.append(f"{value} AS {identifier(column)}")
    return (f"SELECT {', '.join(columns)}, _source_kind FROM {source} "
            f"WHERE _drop = '{drop}'")


def append_select(entry, source, hwm, lineage):
    values = {"transaction_key": f"{hwm} + ROW_NUMBER() OVER (ORDER BY {staged_order()})",
              "lineage_key": str(lineage)}
    projection = []
    for column in entry['columns']:
        value = values.get(column, identifier(column))
        if column in ('total_excluding_tax', 'tax_amount', 'total_including_tax', 'outstanding_balance'):
            value = f"CAST({value} AS STRING)"
        projection.append(f"CAST({value} AS {column_type(column)}) AS {identifier(column)}")
    return f"SELECT {', '.join(projection)} FROM {relation(source)}"


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ('D0', 'D1', 'D2'):
        raise ValueError('Unknown drop')
    cutoff_literal(last_cutoff)
    cutoff_literal(new_cutoff)
    if last_cutoff > new_cutoff:
        raise ValueError('Reversed window')
    if drop == 'D0':
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=['gold.fact_transaction'], replace=True)
        return
    target = f'{catalog}.{gold_schema}.fact_transaction'
    source = relation(f'{catalog}.{silver_schema}.transaction_changes')
    control = Integration(spark, catalog, gold_schema)
    lineage = control.start_lineage('Transaction', new_cutoff)
    hwm = high_water_mark(spark, target, 'transaction_key')
    entry = next(e for e in contract['entries'] if e['table'] == 'gold.fact_transaction' and e['drop'] == 'D0')
    views = ['g14_transaction_stage'] + [f'g14_transaction_lookup_{i}' for i in range(5)]
    try:
        spark.sql(f'CREATE OR REPLACE TEMP VIEW {views[0]} AS ' + stage_select(source, drop)).collect()
        for i, (dimension, key, business_key, natural_id) in enumerate((
                ('customer', 'customer_key', 'wwi_customer_id', 'wwi_customer_id'),
                ('customer', 'bill_to_customer_key', 'wwi_customer_id', 'wwi_bill_to_customer_id'),
                ('supplier', 'supplier_key', 'wwi_supplier_id', 'wwi_supplier_id'),
                ('transaction_type', 'transaction_type_key', 'wwi_transaction_type_id', 'wwi_transaction_type_id'),
                ('payment_method', 'payment_method_key', 'wwi_payment_method_id', 'wwi_payment_method_id'))):
            lookup = lookup_dim_key(
                f'{catalog}.{gold_schema}.dim_{dimension}', views[i],
                fact_row_key=('_source_kind', 'wwi_customer_transaction_id', 'wwi_supplier_transaction_id'),
                key='customer_key' if key == 'bill_to_customer_key' else key,
                business_key=business_key, natural_id=natural_id,
                timestamp='last_modified_when', output_key=key)
            spark.sql(f'CREATE OR REPLACE TEMP VIEW {views[i + 1]} AS {lookup}').collect()
        names = ', '.join(identifier(c) for c in entry['columns'])
        spark.sql(f'INSERT INTO {relation(target)} ({names}) ' +
                  append_select(entry, views[-1], hwm, lineage)).collect()
        control.complete_lineage(lineage)
    finally:
        for view in reversed(views):
            spark.sql(f'DROP VIEW IF EXISTS {view}').collect()


if __name__ == '__main__':
    contract = json.loads((Path.cwd().parents[2] / 'parity/contract.json').read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ('catalog', 'gold_schema', 'silver_schema', 'golden_path',
                            'drop', 'last_cutoff', 'new_cutoff')})
