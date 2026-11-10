import json
import sys
import sqlite3
import re
from collections import Counter
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/sql/units'))
from purchase_core import (COLUMNS, PROJECT, SOURCES, apply_batch, cutoff, extract,
                           column_type, input_path, manifest, read_csv, replay)
from purchase_history import completed_drop_sql, snapshot_sql


@pytest.fixture(scope='module')
def replayed():
    spec = manifest(ROOT / 'golden')
    return spec, replay(ROOT / 'golden', spec)


def multiset(rows, columns):
    return Counter(tuple(r[c] for c in columns) for r in rows)


def test_real_golden_extract_and_accumulation(replayed):
    spec, states = replayed
    accumulated = []
    frozen = None
    for n, expected_size in ((1, 57), (2, 113)):
        drop = f'D{n}'
        rows, orphans = extract(states, spec, drop)
        assert not orphans
        assert len(rows) == (57 if n == 1 else 56)
        assert [r['_staged_order'] for r in rows] == list(range(1, len(rows) + 1))
        assert all(len(r['last_modified_when'].split('.')[1]) == 7 for r in rows)
        assert all(r['is_order_finalized'] in ('0', '1') for r in rows)
        accumulated.extend(rows)
        assert len(accumulated) == expected_size
        if n == 1:
            frozen = [dict(r) for r in rows]
        else:
            assert accumulated[:57] == frozen
        previous = read_csv(ROOT / 'golden', f'outputs/D{n-1}/tables/Fact.Purchase.csv')
        current = read_csv(ROOT / 'golden', f'outputs/{drop}/tables/Fact.Purchase.csv')
        lineage = {r['lineage_key'] for r in previous}
        inserted = [r for r in current if r['lineage_key'] not in lineage]
        assert multiset(accumulated, PROJECT) == multiset(inserted, PROJECT)
        parents = {r['wwi_purchase_order_id'] for r in accumulated}
        keys = {r['purchase_key'] for r in previous}
        retained = [r for r in current if r['purchase_key'] in keys]
        unchanged = [r for r in previous if r['wwi_purchase_order_id'] not in parents]
        assert multiset(retained, previous[0]) == multiset(unchanged, previous[0])
        replaced = {r['purchase_key'] for r in previous if r['wwi_purchase_order_id'] in parents}
        assert not replaced.intersection(r['purchase_key'] for r in current)
        assert multiset(accumulated + [accumulated[0]], PROJECT) != multiset(inserted, PROJECT)
        damaged = [dict(r) for r in accumulated]
        damaged[0]['ordered_quantity'] = '-1'
        assert multiset(damaged, PROJECT) != multiset(inserted, PROJECT)


def test_pk_delete_then_upsert_and_empty():
    old = [{'id': '1', 'v': ''}, {'id': '2', 'v': None}]
    assert apply_batch(old, [], [], ['id', 'v'], ['id']) == old
    assert apply_batch(old, [{'id': '1', 'v': 'new'}], [{'id': '1'}],
                       ['id', 'v'], ['id']) == [old[1], {'id': '1', 'v': 'new'}]
    assert apply_batch([], [], [], ['id'], ['id']) == []


def test_archive_multiset_deletes():
    row = {'id': '1', 'v': None}
    assert apply_batch([row, row], [row], [row], ['id', 'v'], []) == [row, row]
    assert apply_batch([row, row], [], [row], ['id', 'v'], []) == [row]
    with pytest.raises(ValueError, match='archive deletion'):
        apply_batch([], [], [row], ['id', 'v'], [])


def test_csv_empty_and_null(tmp_path):
    (tmp_path / 'rows.csv').write_text('a,b\n,\\N\n')
    assert read_csv(tmp_path, 'rows.csv') == [{'a': '', 'b': None}]
    with pytest.raises(FileNotFoundError):
        read_csv(tmp_path, 'missing.csv')


def test_bronze_types():
    assert column_type('TaxRate') == 'DECIMAL(18,3)'
    assert column_type('IsOrderLineFinalized') == 'INT'
    assert column_type('LastEditedWhen') == 'STRING'
    assert column_type('ValidFrom') == 'STRING'


def test_empty_window_and_orphan(replayed):
    spec, states = replayed
    empty = dict(states)
    empty['D1', 'Purchasing.PurchaseOrderLines'] = []
    assert extract(empty, spec, 'D1') == ([], [])
    missing = dict(states)
    missing['D1', 'Warehouse.StockItems'] = []
    rows, orphans = extract(missing, spec, 'D1')
    assert not rows and len(orphans) == 57
    missing['D1', 'Purchasing.PurchaseOrders'] = []
    assert extract(missing, spec, 'D1')[1]


def test_window_bounds(replayed):
    spec, states = replayed
    original, _ = extract(states, spec, 'D1')
    line_id = original[0]['purchase_order_line_id']
    line = next(dict(r) for r in states['D1', 'Purchasing.PurchaseOrderLines']
                if r['PurchaseOrderLineID'] == line_id)
    order = next(dict(r) for r in states['D1', 'Purchasing.PurchaseOrders']
                 if r['PurchaseOrderID'] == line['PurchaseOrderID'])
    test = dict(states)
    test['D1', 'Purchasing.PurchaseOrderLines'] = [line]
    test['D1', 'Purchasing.PurchaseOrders'] = [order]
    lower = cutoff(spec['drops'][1], 'last_cutoff')
    upper = cutoff(spec['drops'][1], 'new_cutoff')
    order['LastEditedWhen'] = lower
    line['LastEditedWhen'] = lower
    assert extract(test, spec, 'D1') == ([], [])
    line['LastEditedWhen'] = upper
    assert len(extract(test, spec, 'D1')[0]) == 1
    order['LastEditedWhen'] = '2016-06-14 00:00:00.0000001'
    assert extract(test, spec, 'D1') == ([], [])


def test_native_order_and_resources():
    native = json.loads((ROOT / 'golden/native_destination_columns.json').read_text())['Purchase']
    assert native['Is Order Line Finalized'] == 'Is Order Finalized'
    assert COLUMNS[6] == 'is_order_finalized'
    resources = yaml.safe_load((ROOT / 'resources/wwi_purchase_to_silver.yml').read_text())['resources']
    pipeline = resources['pipelines']['purchase_silver_pipeline']
    assert pipeline['serverless'] and pipeline['schema'] == '${var.silver_schema}'
    job = resources['jobs']['wwi_purchase_to_silver']
    assert [t['task_key'] for t in job['tasks']] == ['purchase_pipeline', 'parity_verify']
    assert job['tasks'][0]['pipeline_task']['full_refresh']
    assert [p['name'] for p in job['parameters']] == ['drop', 'last_cutoff', 'new_cutoff']
    assert job['tasks'][1]['notebook_task']['base_parameters']['drop'] == '{{job.parameters.drop}}'
    for t in job['tasks']:
        if 'notebook_task' in t:
            assert (ROOT / 'resources' / t['notebook_task']['notebook_path']).exists()
    registry = json.loads((ROOT / 'src/sql/units/registry.json').read_text())['wwi_purchase_to_silver']
    assert registry['outputs'] == [] and registry['depends_on'] == ['bronze_landing_replay']
    assert set(registry['owns']) == {'silver.purchase_changes', 'silver.purchase_staging',
                                     'silver.purchase_changes_quarantine', 'silver.purchase_parity_results'}
    assert not (ROOT / 'src/sql/units/purchase_bronze.py').exists()
    pipeline_text = (ROOT / 'src/sql/units/purchase_pipeline.py').read_text()
    assert 'expect_or_drop' not in pipeline_text and 'expect_or_fail' not in pipeline_text
    assert '@dp.expect_all(rules)' in pipeline_text
    verify = (ROOT / 'src/sql/units/purchase_verify.py').read_text()
    assert 'EXCEPT ALL' in verify and 'raise_error' in verify


def test_pk_history_query_against_real_golden(replayed):
    spec, expected_states = replayed
    for source, table in SOURCES.items():
        info = spec['files'][input_path('D0', source)]
        if not info['primary_key']:
            continue
        columns = info['columns']
        with sqlite3.connect(':memory:') as connection:
            connection.execute('CREATE TABLE raw (' + ', '.join(f'`{c}` TEXT' for c in columns)
                               + ', _drop TEXT, _op TEXT)')
            for drop in ('D0', 'D1', 'D2'):
                path = input_path(drop, source)
                for file, op in ((path, 'base' if drop == 'D0' else 'upsert'),
                                 (path[:-4] + '.deleted.csv', 'delete')):
                    if file not in spec['files']:
                        continue
                    rows = [tuple(r.get(c) for c in columns) + (drop, op)
                            for r in read_csv(ROOT / 'golden', file)]
                    connection.executemany('INSERT INTO raw VALUES ('
                                           + ','.join('?' for _ in range(len(columns) + 2)) + ')', rows)
            for drop in ('D0', 'D1', 'D2'):
                sql = snapshot_sql('catalog', 'bronze', source, drop, spec)
                sql = sql.replace(f'`catalog`.`bronze`.`{table}_raw`', 'raw')
                # SQLite executes the same event-selection SQL; retain CSV representations for comparison.
                sql = re.sub(r'CAST\(`([^`]+)` AS [^)]+(?:\(\d+,\d+\))?\) AS `\1`',
                             r'`\1`', sql)
                # Decimal casts contain a closing precision parenthesis.
                sql = re.sub(r'CAST\(`([^`]+)` AS DECIMAL\(\d+,\d+\)\) AS `\1`', r'`\1`', sql)
                actual = [dict(zip(columns, row[:-1])) for row in connection.execute(sql)]
                assert multiset(actual, columns) == multiset(expected_states[drop, source], columns)


def test_history_sql_guards_and_multisets(replayed):
    spec, _ = replayed
    sql = snapshot_sql('c', 'b', 'Warehouse.StockItems_Archive', 'D1', spec)
    assert "_drop <= 'D1'" in sql and "_op = 'delete' THEN -1 ELSE 1" in sql
    assert 'GROUP BY `StockItemID`, `StockItemName`' in sql
    assert 'array_repeat' in sql and 'raise_error' in sql
    with pytest.raises(ValueError):
        snapshot_sql('c', 'b', 'Warehouse.StockItems', 'D3', spec)
    assert "status = 'PASS'" in completed_drop_sql('c', 'b', 'D0')
