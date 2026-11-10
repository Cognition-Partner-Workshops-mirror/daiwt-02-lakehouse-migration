import json
import sqlite3
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from classify_changes import classify
from sql.framework import lookup_dim_key
from sql.units.p5_gold_seed.integration import ENTITIES
from sql.units.wwi_transaction_to_silver import transaction_s13 as s13
from sql.units.wwi_fact_transaction_to_gold.job import append_select, run, stage_select, staged_order
from sql.units.wwi_fact_transaction_to_gold.verify_transaction import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / 'src/parity/contract.json').read_text())
ENTRY = next(e for e in CONTRACT['entries'] if e['table'] == 'gold.fact_transaction' and e['drop'] == 'D0')
ARGS = dict(catalog='dev', gold_schema='gold_pr_0', silver_schema='silver_pr_0',
            golden_path='/golden', last_cutoff='2016-06-06 12:31:17.0000000',
            new_cutoff='2016-06-14 00:00:00.0000000')


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self, fail=False):
        self.statements, self.fail = [], fail

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith('SELECT lineage_key'):
            return Result([(i + 14, name) for i, name in enumerate(ENTITIES)])
        if statement.startswith('SELECT coalesce(max'):
            return Result([(99585,)])
        if self.fail and statement.startswith('INSERT INTO'):
            raise RuntimeError('write failed')
        return Result()


def test_seed():
    spark = Spark()
    run(spark, CONTRACT, **ARGS, drop='D0')
    assert len(spark.statements) == 1
    assert 'CREATE OR REPLACE TABLE' in spark.statements[0]
    assert '/outputs/D0/tables/Fact.Transaction.csv' in spark.statements[0]


@pytest.mark.parametrize('drop', ['D1', 'D2'])
def test_append_and_lineage(drop):
    spark = Spark()
    run(spark, CONTRACT, **ARGS, drop=drop)
    sql = '\n'.join(spark.statements)
    assert f"WHERE _drop = '{drop}'" in sql
    assert '99585 + ROW_NUMBER()' in sql
    assert staged_order() in sql
    assert 'CAST(26 AS BIGINT)' in sql
    assert 'CAST(CAST(`total_including_tax` AS STRING) AS DECIMAL(18,2))' in sql
    assert sql.count('AND f.`last_modified_when` > d.`valid_from`') == 5
    assert sql.count('AND f.`last_modified_when` <= d.`valid_to`') == 5
    assert 'f.`wwi_bill_to_customer_id`' in sql
    assert not any(s.startswith(('DELETE', 'MERGE')) for s in spark.statements)
    assert sum(s.startswith('INSERT INTO') for s in spark.statements) == 1
    assert 'WHERE lineage_key = 26' in sql
    assert spark.statements[-6:] == [f'DROP VIEW IF EXISTS g14_transaction_lookup_{i}' for i in reversed(range(5))] + ['DROP VIEW IF EXISTS g14_transaction_stage']


def test_failed_write_cleanup_without_completion():
    spark = Spark(fail=True)
    with pytest.raises(RuntimeError, match='write failed'):
        run(spark, CONTRACT, **ARGS, drop='D1')
    assert not any(s.startswith('UPDATE') for s in spark.statements)
    assert spark.statements[-1] == 'DROP VIEW IF EXISTS g14_transaction_stage'


@pytest.mark.parametrize('override', [dict(drop='D3'), dict(last_cutoff='bad'),
                                     dict(last_cutoff='2017-01-01 00:00:00.0000000')])
def test_invalid_window(override):
    spark = Spark()
    with pytest.raises(ValueError):
        run(spark, CONTRACT, **(ARGS | dict(drop='D1') | override))
    assert spark.statements == []


def normalized(row, columns):
    return tuple(None if row[c] is None else
                 Decimal(str(row[c])).quantize(Decimal('.01')) if c in
                 ('total_excluding_tax', 'tax_amount', 'total_including_tax', 'outstanding_balance')
                 else str(row[c]) for c in columns)


def execute_lookup(db, dimension, facts, key, business_key, natural_id, output_key):
    query = lookup_dim_key(dimension, facts, fact_row_key=('_source_kind', 'wwi_customer_transaction_id', 'wwi_supplier_transaction_id'),
                           key=key, business_key=business_key, natural_id=natural_id,
                           timestamp='last_modified_when', output_key=output_key)
    select, qualify = query.split('\nQUALIFY ')
    select = select.replace('\nFROM ', ', ' + qualify.removesuffix(' = 1') + ' AS _rn FROM ', 1)
    return list(db.execute(f'SELECT * FROM ({select}) WHERE _rn = 1'))


def resolved_rows(extracted, dimensions):
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    columns = [n.lower().replace(' ', '_') for n in s13.NAMES] + ['_source_kind']
    db.execute('CREATE TABLE stage (' + ', '.join(f'`{c}`' for c in columns) + ')')
    db.executemany('INSERT INTO stage VALUES (' + ','.join('?' for _ in columns) + ')',
                   [[r[n] for n in s13.NAMES] + ['customer' if r[s13.NAMES[1]] is not None else 'supplier'] for r in extracted])
    for dimension, natural, output in [('Customer', 'customer', 'customer'), ('Customer', 'customer', 'bill_to_customer'),
                                        ('Supplier', 'supplier', 'supplier'), ('Transaction_Type', 'transaction_type', 'transaction_type'),
                                        ('Payment_Method', 'payment_method', 'payment_method')]:
        rows = dimensions[dimension]
        db.execute('DROP TABLE IF EXISTS dimension')
        db.execute('CREATE TABLE dimension (' + ','.join(f'`{c}`' for c in rows[0]) + ')')
        db.executemany('INSERT INTO dimension VALUES (' + ','.join('?' for _ in rows[0]) + ')', [list(r.values()) for r in rows])
        result = execute_lookup(db, 'dimension', 'stage', natural + '_key', 'wwi_' + natural + '_id',
                                'wwi_' + output + '_id', output + '_key')
        columns.append(output + '_key')
        db.execute('DROP TABLE stage')
        db.execute('CREATE TABLE stage (' + ','.join(f'`{c}`' for c in columns) + ')')
        db.executemany('INSERT INTO stage VALUES (' + ','.join('?' for _ in columns) + ')', [[r[c] for c in columns] for r in result])
    return db


def golden_replay():
    manifest = json.loads((ROOT / 'golden/manifest.json').read_text())
    states = {source: {} for source in s13.SOURCES}
    hwm = 0
    reports = []
    for drop in manifest['drops']:
        d = drop['name']
        for source in states:
            path = f'inputs/{d}/WideWorldImporters.{source}.csv'
            deleted = path[:-4] + '.deleted.csv'
            s13.replay(states[source], s13.read_csv(ROOT / 'golden' / path),
                       s13.read_csv(ROOT / 'golden' / deleted) if deleted in manifest['files'] else [],
                       manifest['files'][path]['primary_key'][0])
        expected = s13.read_csv(ROOT / f'golden/outputs/{d}/tables/Fact.Transaction.csv')
        if d != 'D0':
            new = [r for r in expected if int(r['transaction_key']) > hwm]
            dims = {name: s13.read_csv(ROOT / f'golden/outputs/{d}/tables/Dimension.{name}.csv')
                    for name in ['Customer', 'Supplier', 'Transaction_Type', 'Payment_Method']}
            db = resolved_rows(list(s13.extract_python(states, drop)), dims)
            lineage = next(r['lineage_key'] for r in s13.read_csv(ROOT / f'golden/outputs/{d}/tables/Integration.Lineage.csv')
                           if r['table_name'] == 'Transaction' and r['source_system_cutoff_time'] == s13.cutoff(drop['new_cutoff']))
            query = append_select(ENTRY, 'stage', hwm, int(lineage)).replace(' AS STRING', ' AS TEXT')
            query = query.replace('CAST(`date_key` AS DATE)', '`date_key`')
            actual = list(db.execute(query))
            cols = ENTRY['columns'][1:]
            match = Counter(normalized(r, cols) for r in actual) == Counter(normalized(r, cols) for r in new)
            full_matches = sum((Counter(normalized(r, ENTRY['columns']) for r in actual) &
                                Counter(normalized(r, ENTRY['columns']) for r in new)).values())
            reports.append((d, len(actual), match, full_matches))
            db.close()
        hwm = max(int(r['transaction_key']) for r in expected)
    return reports


def test_offline_values():
    reports = golden_replay()
    assert [(d, size, match) for d, size, match, keys in reports] == [('D1', 705, True), ('D2', 842, True)]
    assert all(0 < keys < size for d, size, match, keys in reports)


def test_empty_and_lookup_boundaries():
    dims = {name: [{'wwi_' + natural + '_id': '1', natural + '_key': '7',
                    'valid_from': '2016-01-01 00:00:00.0000000', 'valid_to': '2016-06-01 00:00:00.0000000'}]
            for name, natural in [('Customer', 'customer'), ('Supplier', 'supplier'),
                                  ('Transaction_Type', 'transaction_type'), ('Payment_Method', 'payment_method')]}
    db = resolved_rows([], dims)
    assert list(db.execute(append_select(ENTRY, 'stage', 9, 26))) == []
    for timestamp, expected in [('2016-01-01 00:00:00.0000000', 0), ('2016-06-01 00:00:00.0000000', 7),
                                ('2016-06-01 00:00:00.0000001', 0)]:
        row = dict.fromkeys(s13.NAMES)
        row.update({'Date Key': '2016-01-01', 'WWI Customer Transaction ID': '1', 'WWI Customer ID': '1',
                    'Last Modified When': timestamp})
        db = resolved_rows([row], dims)
        result = list(db.execute(append_select(ENTRY, 'stage', 9, 26)))[0]
        assert int(result['customer_key']) == expected
        assert result['supplier_key'] == 0
        assert result['transaction_key'] == 10


def test_parity_success_and_failure(monkeypatch):
    args = {k: ARGS[k] for k in ('catalog', 'gold_schema', 'golden_path')}
    module = 'sql.units.wwi_fact_transaction_to_gold.verify_transaction.compare_entry'
    monkeypatch.setattr(module, lambda *a: dict(status='PASS'))
    assert parity(Spark(), CONTRACT, **args, drop='D0')['status'] == 'PASS'
    monkeypatch.setattr(module, lambda *a: dict(status='FAIL', reason='row mismatch'))
    with pytest.raises(RuntimeError, match='row mismatch'):
        parity(Spark(), CONTRACT, **args, drop='D1')


def test_interfaces():
    registry = json.loads((ROOT / 'src/sql/units/registry.json').read_text())
    unit = 'wwi_fact_transaction_to_gold'
    assert classify(registry, [f'resources/{unit}.job.yml']) == [unit]
    for upstream in registry[unit]['depends_on']:
        assert unit in classify(registry, [f'resources/{upstream}.job.yml'])
    job = yaml.safe_load((ROOT / f'resources/{unit}.job.yml').read_text())['resources']['jobs'][unit]
    assert 'permissions' not in job
    assert [t['task_key'] for t in job['tasks']] == ['load', 'parity']
    assert job['tasks'][1]['depends_on'] == [dict(task_key='load')]
    assert {p['name'] for p in job['parameters']} == {'drop', 'last_cutoff', 'new_cutoff'}
    sql = stage_select('silver', 'D2')
    assert '`Last Modified When`' in sql and 'date_format' in sql
    assert '`WWI Bill To Customer ID` AS `wwi_bill_to_customer_id`' in sql
