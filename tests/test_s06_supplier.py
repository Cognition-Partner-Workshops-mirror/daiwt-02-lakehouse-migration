import importlib.util
import json
import runpy
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('supplier_logic', ROOT / 'src/sql/units/s06_supplier/logic.py')
logic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logic)


def fixture_rows():
    s = dict(SupplierID='1', SupplierName='Supplier', SupplierCategoryID='2',
             PrimaryContactPersonID='3', SupplierReference='Ref', PaymentDays='7',
             DeliveryPostalCode='123', ValidFrom='2016-01-01 00:00:00.0000000', ValidTo=logic.END)
    c = dict(SupplierCategoryID='2', SupplierCategoryName='Category',
             ValidFrom='2015-01-01 00:00:00.0000000', ValidTo=logic.END)
    p = dict(PersonID='3', FullName='Contact', ValidFrom='2015-01-01 00:00:00.0000000', ValidTo=logic.END)
    return s, c, p


def test_replay():
    a, b = {'id': '1', 'v': 'a'}, {'id': '1', 'v': 'b'}
    assert logic.replay([a], [b], [a], 'id') == [b]
    assert logic.replay([a], [], [], 'id') == [a]
    assert logic.replay([a, a], [b], [a], None) == [a, b]
    with pytest.raises(ValueError, match='archive delete'):
        logic.replay([], [], [a], None)


def test_walk_boundaries_ties_and_contacts():
    s, c, p = fixture_rows()
    last, new = '2015-12-31 00:00:00.0000000', '2016-02-01 00:00:00.0000000'
    assert len(logic.walk([s], [c], [p], last, s['ValidFrom'])) == 1
    assert not logic.walk([s], [c], [p], s['ValidFrom'], new)
    assert not logic.walk([s], [c], [], last, new)
    changed_c = dict(c, ValidFrom='2016-01-15 00:00:00.0000000')
    old_c = dict(c, ValidTo=changed_c['ValidFrom'])
    rows = logic.walk([s], [old_c, changed_c], [p], last, new)
    assert len(rows) == 2
    assert all(r[7] == s['ValidFrom'] and r[8] == logic.END for r in rows)
    changed_p = dict(p, ValidFrom='2016-01-15 00:00:00.0000000')
    assert not logic.walk([s], [c], [p, changed_p], s['ValidFrom'], new)
    second_s = dict(s, ValidFrom=changed_p['ValidFrom'])
    rows = logic.walk([dict(s, ValidTo=second_s['ValidFrom']), second_s], [c], [p], last, new)
    assert rows[0][8] == second_s['ValidFrom']


def test_initial_date_exclusion():
    s, c, p = fixture_rows()
    t = '2020-01-01 00:00:00.0000000'
    assert not logic.walk([s], [dict(c, ValidFrom=t)], [p], '2019', '2021')
    assert len(logic.walk([dict(s, ValidFrom=t)], [dict(c, ValidFrom=t)], [p], '2019', '2021')) == 1


def test_golden_all_drops():
    state = {source: [] for source in logic.SOURCES}
    manifest = json.loads((ROOT / 'golden/manifest.json').read_text())
    for drop in ('D0', 'D1', 'D2'):
        for source, (_, key) in logic.SOURCES.items():
            path = ROOT / 'golden/inputs' / drop / f'WideWorldImporters.{source}.csv'
            deleted = path.with_suffix('.deleted.csv')
            state[source] = logic.replay(state[source], logic.read_csv(path),
                                         logic.read_csv(deleted) if deleted.exists() else [], key)
        def history(source):
            return state[source] + state[source + '_Archive']
        rows = logic.walk(history('Purchasing.Suppliers'), history('Purchasing.SupplierCategories'),
                          history('Application.People'), *logic.cutoffs(manifest, drop))
        assert Counter(rows) == logic.expected(ROOT / 'golden', drop) == Counter()
        assert not [r for r in rows if any(v == '\\N' for v in r)]


def test_runner_contract():
    import yaml
    resource = yaml.safe_load((ROOT / 'resources/s06_supplier.yml').read_text())
    job = resource['resources']['jobs']['s06_supplier_job']
    assert {p['name'] for p in job['parameters']} == {'drop', 'last_cutoff', 'new_cutoff'}
    tasks = job['tasks']
    assert [t['task_key'] for t in tasks] == ['run_window', 'silver', 'parity']
    registry = json.loads((ROOT / 'src/sql/units/registry.json').read_text())['s06_supplier']
    assert registry['depends_on'] == ['bronze_landing_replay', 'silver_temporal_views']
    assert registry['outputs'] == []
    assert not any('raw' in table for table in registry['owns'])


@pytest.mark.parametrize('rows,quarantine,fail', [([], 0, False),
    ([dict(zip(logic.COLUMNS, ['unexpected'] * 9))], 0, True), ([], 1, True)])
def test_runtime_parity(monkeypatch, rows, quarantine, fail):
    monkeypatch.setitem(sys.modules, 'logic', logic)
    parameters = {'catalog': 'test', 'silver_schema': 'silver',
                  'golden_path': str(ROOT / 'golden'), 'drop': 'D1'}
    widgets = SimpleNamespace(text=lambda *args: None, get=parameters.__getitem__)
    def raise_error(query):
        assert 'raise_error' in query
        raise RuntimeError('parity failure')
    spark = SimpleNamespace(table=lambda name: SimpleNamespace(collect=lambda: rows, count=lambda: quarantine),
                            sql=raise_error)
    def run():
        runpy.run_path(str(ROOT / 'src/sql/units/s06_supplier/parity.py'),
                       init_globals={'spark': spark, 'dbutils': SimpleNamespace(widgets=widgets)})
    if fail:
        with pytest.raises(RuntimeError, match='parity failure'):
            run()
    else:
        run()


def test_expected_multiset_changes(tmp_path):
    import csv
    for drop, values in [('D0', ['old', 'same']), ('D1', ['new', 'same', 'same'])]:
        target = tmp_path / 'outputs' / drop / 'tables'
        target.mkdir(parents=True)
        with (target / 'Dimension.Supplier.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=logic.COLUMNS)
            writer.writeheader()
            for value in values:
                writer.writerow(dict.fromkeys(logic.COLUMNS, value))
    assert logic.expected(tmp_path, 'D1') == Counter({('new',) * 9: 1, ('same',) * 9: 1})


@pytest.mark.parametrize('drop', ['D0', 'D2', 'BAD'])
def test_run_window_notebook(monkeypatch, drop):
    monkeypatch.setitem(sys.modules, 'logic', logic)
    saved = {}
    class Frame:
        def __init__(self, rows, schema):
            self.rows, self.schema, self.write = rows, schema, self
        def mode(self, value):
            return self
        def option(self, *args):
            return self
        def saveAsTable(self, name):
            saved[name] = self.rows
    parameters = dict(catalog='test', bronze_schema='bronze', golden_path=str(ROOT / 'golden'),
                      drop=drop, last_cutoff='last', new_cutoff='new')
    widgets = SimpleNamespace(text=lambda *args: None, get=parameters.__getitem__)
    def run():
        runpy.run_path(str(ROOT / 'src/sql/units/s06_supplier/run_window.py'), init_globals={
            'spark': SimpleNamespace(createDataFrame=Frame), 'dbutils': SimpleNamespace(widgets=widgets)})
    if drop == 'BAD':
        with pytest.raises(ValueError, match='invalid drop'):
            run()
        assert not saved
        return
    run()
    assert len(saved) == 1
    assert saved['test.bronze.s06_supplier_run_window'] == [(drop, 'last', 'new')]


def test_spark_query_matches_walk():
    pytest.importorskip('pyspark')
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master('local[1]').appName('S06').getOrCreate()
    try:
        s, c, p = fixture_rows()
        t = '2016-01-15 00:00:00.0000001'
        suppliers = [dict(s, ValidTo=t), dict(s, ValidFrom=t, SupplierName='Changed')]
        categories = [dict(c, ValidTo=t), dict(c, ValidFrom=t)]
        for table, rows in [('purchasing_suppliers', suppliers),
                            ('purchasing_supplier_categories', categories), ('application_people', [p])]:
            rows = [dict(r, ValidFrom_str=r['ValidFrom'], ValidTo_str=r['ValidTo'],
                         ValidFrom='1900-01-01', ValidTo='1900-01-02') for r in rows]
            schema = ','.join(f'{name} STRING' for name in rows[0])
            spark.createDataFrame([tuple(r.values()) for r in rows], schema).createOrReplaceTempView(table)
            spark.createDataFrame([], schema).createOrReplaceTempView(table + '_archive')
            spark.sql(f"SELECT *, ValidFrom_str valid_from, ValidTo_str valid_to FROM {table} "
                      f"UNION ALL SELECT *, ValidFrom_str valid_from, ValidTo_str valid_to FROM {table}_archive"
                      ).createOrReplaceTempView(table + '_history')
        spark.sql('SELECT DISTINCT source_table, valid_from FROM (' + ' UNION ALL '.join(
            f"SELECT '{table}' source_table, valid_from FROM {table}_history" for table in
            ('purchasing_suppliers', 'purchasing_supplier_categories', 'application_people')) + ')'
                  ).createOrReplaceTempView('temporal_change_events')
        spark.createDataFrame([('D1', '2015-12-31', '2016-02-01')],
                              'drop STRING, last_cutoff STRING, new_cutoff STRING').createOrReplaceTempView('run_window')
        query = logic.change_sql('unused', window_table='run_window').replace('unused.', '')
        actual = Counter(tuple(r[c] for c in logic.COLUMNS) for r in spark.sql(query).collect())
        assert actual == Counter(logic.walk(suppliers, categories, [p], '2015-12-31', '2016-02-01'))
        assert len(list(actual.elements())) == 3
        assert not spark.sql(logic.change_sql('unused', '2016-02-01', '2016-02-01').replace('unused.', '')).collect()
    finally:
        spark.stop()
