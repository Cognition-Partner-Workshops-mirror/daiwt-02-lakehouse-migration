import importlib.util
import json
import re
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('transaction_s13', ROOT / 'src/sql/units/wwi_transaction_to_silver/transaction_s13.py')
s13 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s13)


def test_native_contract():
    declaration = json.loads((ROOT / 'golden/native_result_sets.json').read_text())['Transaction']
    assert re.findall(r'\[([^]]+)\]', declaration) == s13.NAMES
    assert len(s13.TYPES) == len(s13.NAMES)


def test_drop_parity():
    golden = ROOT / 'golden'
    manifest = json.loads((golden / 'manifest.json').read_text())
    states = {source: {} for source in s13.SOURCES}
    previous = Counter()
    seen = set()
    repeated = 0
    for drop, size, warnings in zip(manifest['drops'], [99585, 705, 842], [27003, 172, 191]):
        d = drop['name']
        for source in states:
            path = f'inputs/{d}/WideWorldImporters.{source}.csv'
            metadata = manifest['files'][path]
            deleted = path[:-4] + '.deleted.csv'
            s13.replay(states[source], s13.read_csv(golden / path),
                       s13.read_csv(golden / deleted) if deleted in manifest['files'] else [],
                       metadata['primary_key'][0])
            assert len(states[source]) == metadata['full_row_count']
        rows = list(s13.extract_python(states, drop))
        full = Counter(tuple(row[c] for c in s13.GOLD) for row in
                       s13.read_csv(golden / f'outputs/{d}/tables/Fact.Transaction.csv'))
        assert sum((full - previous).values()) == size
        assert s13.project_python(rows) == full - previous
        assert len(rows) == size
        assert sum(Decimal(r['Total Excluding Tax']) + Decimal(r['Tax Amount']) != Decimal(r['Total Including Tax']) for r in rows) == warnings
        for row in rows:
            customer, supplier = row[s13.NAMES[1]], row[s13.NAMES[2]]
            assert (customer is None) != (supplier is None)
            assert row['Is Finalized'] in ('0', '1')
            assert row['Outstanding Balance'] is not None
            if customer is not None:
                assert row['WWI Customer ID'] is not None
                assert row['WWI Bill To Customer ID'] is not None
                assert row['WWI Supplier ID'] is None
            else:
                assert row['WWI Supplier ID'] is not None
                assert row['WWI Customer ID'] is None
            identity = (customer, supplier)
            repeated += identity in seen
            seen.add(identity)
        previous = full
    assert repeated > 0


def test_replay_empty_delete_and_upsert():
    state = {'1': {'id': '1', 'value': 'old'}}
    s13.replay(state, [], [], 'id')
    assert state['1']['value'] == 'old'
    s13.replay(state, [{'id': '1', 'value': 'new'}], [{'id': '1'}], 'id')
    assert state == {'1': {'id': '1', 'value': 'new'}}
    s13.replay(state, [], [{'id': '1'}], 'id')
    assert state == {}
    with pytest.raises(KeyError):
        s13.replay(state, [], [{'id': 'missing'}], 'id')
    assert list(s13.extract_python({source: {} for source in s13.SOURCES},
                                  {'name': 'D0', 'new_cutoff': '2016-06-06 12:31:17'})) == []


def test_window_boundaries_and_customer_roles():
    row = dict(CustomerTransactionID='1', CustomerID='2', InvoiceID='3',
               TransactionDate='2016-06-07', AmountExcludingTax='1.00', TaxAmount='0.20',
               TransactionAmount='1.20', OutstandingBalance='-1.20', IsFinalized='0',
               TransactionTypeID='1', PaymentMethodID=None,
               LastEditedWhen='2016-06-14 00:00:00.0000000')
    states = {source: {} for source in s13.SOURCES}
    states['Sales.CustomerTransactions']['1'] = row
    states['Sales.Invoices']['3'] = {'CustomerID': '4'}
    drop = {'name': 'D1', 'last_cutoff': '2016-06-06 12:31:17', 'new_cutoff': '2016-06-14 00:00:00'}
    extracted = list(s13.extract_python(states, drop))
    assert extracted[0]['WWI Customer ID'] == '4'
    assert extracted[0]['WWI Bill To Customer ID'] == '2'
    states['Sales.Invoices'].clear()
    assert next(s13.extract_python(states, drop))['WWI Customer ID'] == '2'
    row['LastEditedWhen'] = '2016-06-06 12:31:17.0000000'
    assert list(s13.extract_python(states, drop)) == []


def test_sql_retains_drop_and_union_all():
    sql = s13.extract_sql('dev.bronze', [
        {'name': 'D0', 'new_cutoff': '2016-06-06 12:31:17'},
        {'name': 'D1', 'last_cutoff': '2016-06-06 12:31:17', 'new_cutoff': '2016-06-14 00:00:00'}])
    assert sql.count(' UNION ALL ') == 3
    assert "_drop <= 'D0'" in sql
    assert "_drop <= 'D1'" in sql
    assert 'coalesce(i.CustomerID, ct.CustomerID)' in sql
    assert '_row_ordinal' not in sql
    assert sql.count('bronze_replay_log') == 6
    assert sql.count("status = 'PASS'") == 6
    assert "CASE WHEN _op = 'upsert' THEN 1 ELSE 0 END DESC" in sql
    assert "_op <> 'delete'" in sql


def test_platform_interfaces():
    from registry import load
    registry = load()
    entry = registry['wwi_transaction_to_silver']
    assert entry == dict(job='wwi_transaction_to_silver', outputs=[],
                         owns=['silver.transaction_changes', 'silver.transaction_quarantine'],
                         depends_on=['bronze_landing_replay'])
    p4 = json.loads((ROOT / 'src/bronze/tables.json').read_text())
    tables = {t['source_table']: t for t in p4['tables']}
    for source, table in s13.SOURCES.items():
        assert tables[source]['table'] == table
        assert tables[source]['primary_key']
    resource = yaml.safe_load((ROOT / 'resources/wwi_transaction_to_silver.job.yml').read_text())
    job = resource['resources']['jobs']['wwi_transaction_to_silver']
    assert [t['task_key'] for t in job['tasks']] == ['silver', 'parity']
    assert {p['name'] for p in job['parameters']} == {'drop', 'last_cutoff', 'new_cutoff'}
    params = job['tasks'][1]['notebook_task']['base_parameters']
    for name in ('drop', 'last_cutoff', 'new_cutoff'):
        assert params[name] == '{{job.parameters.' + name + '}}'
