import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('payment_method', ROOT / 'src/sql/units/wwi_payment_method_to_silver/payment_method.py')
pm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pm)


@pytest.mark.parametrize('drop,last,new,count', [
    ('D0', '2012-12-31 00:00:00.0000000', '2016-06-06 12:31:17.0000000', 5),
    ('D1', '2016-06-06 12:31:17.0000000', '2016-06-14 00:00:00.0000000', 0),
    ('D2', '2016-06-14 00:00:00.0000000', '2016-06-21 00:00:00.0000000', 0),
])
def test_golden_baseline_full_history_and_incremental_parity(drop, last, new, count):
    base, _ = pm.replay(ROOT / 'golden', drop)
    archive, _ = pm.replay(ROOT / 'golden', drop, archive=True)
    assert len(base) == 4
    assert len(archive) == 1
    actual = pm.changes(base + archive, last, new)
    assert len(actual) == count
    pm.assert_parity(actual, pm.expected(ROOT / 'golden', drop))


def row(start, end, name='Cash'):
    return dict(zip(pm.SOURCE_COLUMNS, ['1', name, '1', start, end]))


def test_window_excludes_last_includes_new_and_asof_is_end_exclusive():
    history = [row('a', 'b', 'Old'), row('b', 'c', 'New'), row('c', pm.END, 'Later')]
    assert pm.changes(history, 'a', 'b') == [dict(wwi_payment_method_id=1, payment_method='New', valid_from='b', valid_to=pm.END)]
    result = pm.changes(history, '0', 'b')
    assert [r['valid_to'] for r in result] == ['b', pm.END]


def test_empty_and_invalid_windows():
    assert pm.changes([], 'a', 'b') == []
    with pytest.raises(ValueError, match='increase'):
        pm.changes([], 'b', 'a')
    with pytest.raises(ValueError, match='increase'):
        pm.changes([], 'a', 'a')


def test_parity_strict_multiplicity_null_and_seven_digit_end():
    actual = pm.changes([row('2013-01-01 00:00:00.0000000', pm.END)], '2012', '2014')
    pm.assert_parity(actual, actual)
    pm.assert_parity([], [])
    for wrong in [[], actual * 2, [{**actual[0], 'valid_to': pm.END[:-1]}],
                  [{**actual[0], 'payment_method': None}]]:
        with pytest.raises(AssertionError, match='multiset mismatch'):
            pm.assert_parity(actual, wrong)


def write_csv(path, rows):
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=pm.SOURCE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


@pytest.mark.parametrize('archive', [False, True])
def test_replay_deletes_before_upserts_and_keeps_archive_multiplicity(tmp_path, archive):
    name = 'PaymentMethods_Archive' if archive else 'PaymentMethods'
    def path(drop):
        return tmp_path / 'inputs' / drop / f'WideWorldImporters.Application.{name}.csv'
    old = row('a', 'b')
    new = row('b', pm.END)
    write_csv(path('D0'), [old, old] if archive else [old])
    write_csv(path('D1'), [new])
    write_csv(path('D1').with_suffix('.deleted.csv'), [old])
    snapshot, audit = pm.replay(tmp_path, 'D1', archive)
    assert snapshot == ([old, new] if archive else [new])
    assert [r['_op'] for r in audit][-2:] == ['delete', 'upsert']
    write_csv(path('D2'), [])
    bad = row('missing', 'missing') if archive else {**old, 'PaymentMethodID': '99'}
    write_csv(path('D2').with_suffix('.deleted.csv'), [bad])
    with pytest.raises(ValueError, match='does not match'):
        pm.replay(tmp_path, 'D2', archive)
