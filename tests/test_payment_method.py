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
    base = pm.read_csv(ROOT / 'golden/inputs/D0/WideWorldImporters.Application.PaymentMethods.csv')
    archive = pm.read_csv(ROOT / 'golden/inputs/D0/WideWorldImporters.Application.PaymentMethods_Archive.csv')
    for current in pm.DROPS[1:pm.DROPS.index(drop) + 1]:
        for name in ('PaymentMethods', 'PaymentMethods_Archive'):
            assert pm.read_csv(ROOT / f'golden/inputs/{current}/WideWorldImporters.Application.{name}.csv') == []
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


def test_runner_window_d0_override_and_incremental_passthrough():
    assert pm.window('D0', '2016', '2016') == ('D0', '2012-12-31 00:00:00.0000000', '2016')
    assert pm.window('D1', '2016-06-06', '2016-06-14') == ('D1', '2016-06-06', '2016-06-14')
    for args in [('D9', 'a', 'b'), ('D1', '', 'b'), ('D2', 'a', ''), ('D1', 'b', 'a')]:
        with pytest.raises(ValueError, match='Invalid drop or cutoff window'):
            pm.window(*args)


def test_parity_strict_multiplicity_null_and_seven_digit_end():
    actual = pm.changes([row('2013-01-01 00:00:00.0000000', pm.END)], '2012', '2014')
    pm.assert_parity(actual, actual)
    pm.assert_parity([], [])
    for wrong in [[], actual * 2, [{**actual[0], 'valid_to': pm.END[:-1]}],
                  [{**actual[0], 'payment_method': None}]]:
        with pytest.raises(AssertionError, match='multiset mismatch'):
            pm.assert_parity(actual, wrong)
