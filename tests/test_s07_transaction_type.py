import csv
import json
from collections import Counter
from pathlib import Path

import pytest

from src.bronze.bronze_landing_replay import reference_replay
from src.s07.transaction_type import (
    COLUMNS, END_OF_TIME, INITIAL_CUTOFF, SOURCE_COLUMNS, assert_parity, expected_rows, extract, window,
)

ROOT = Path(__file__).resolve().parents[1] / "golden"


def replay(current, upserts, deletes, archive=False):
    spec = {"columns": [{"name": c} for c in SOURCE_COLUMNS],
            "primary_key": [] if archive else ["TransactionTypeID"]}
    def rows(values, columns=SOURCE_COLUMNS):
        return [[row[c] for c in columns] for row in values]
    result = reference_replay(spec, rows(current), rows(upserts),
                              rows(deletes, SOURCE_COLUMNS if archive else ["TransactionTypeID"]))
    return [dict(zip(SOURCE_COLUMNS, row)) for row in result]


def csv_rows(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


def version(identifier, start, end=END_OF_TIME, name="Name"):
    return dict(TransactionTypeID=str(identifier), TransactionTypeName=name, LastEditedBy="1",
                ValidFrom=start, ValidTo=end)


def test_temporal_window_and_strictly_later_valid_to():
    rows = [version(1, "2013-01-01", "2014-01-01", "old"), version(1, "2014-01-01", name="new")]
    assert extract(rows, "2013-01-01", "2014-01-01") == [("1", "new", "2014-01-01", END_OF_TIME)]
    assert extract(rows, "2012-01-01", "2014-01-01") == [
        ("1", "old", "2013-01-01", "2014-01-01"), ("1", "new", "2014-01-01", END_OF_TIME)]
    assert extract(rows, "2014-01-01", "2015-01-01") == []
    duplicate = version(2, "2014-01-01")
    assert extract([duplicate, duplicate], "2013-01-01", "2014-01-01") == [
        ("2", "Name", "2014-01-01", END_OF_TIME)] * 4


def test_zero_length_temporal_version_is_not_visible():
    assert extract([version(1, "2013-01-01", "2013-01-01")], "2012-01-01", "2014-01-01") == []


def test_replay_pk_and_archive_multiplicity():
    old, new = version(1, "2013-01-01"), version(1, "2014-01-01")
    assert replay([old], [new], [{"TransactionTypeID": "1"}]) == [new]
    assert replay([old, old], [new], [old], archive=True) == [old, new]
    assert replay([], [], [], archive=True) == []
    with pytest.raises(ValueError, match="Deleted row absent"):
        replay([], [], [old], archive=True)


def test_parity_enforces_values_multiplicity_and_quarantine():
    row = ("1", "Name", "2013-01-01", END_OF_TIME)
    assert_parity([row], Counter([row]))
    assert_parity([], Counter())
    for actual, expected, quarantine in (([row, row], Counter([row]), 0),
                                          ([], Counter([row]), 0), ([row], Counter([row]), 1)):
        with pytest.raises(AssertionError, match="S07 parity mismatch"):
            assert_parity(actual, expected, quarantine)


def test_golden_d0_d1_d2_parity():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    state = [[], []]
    previous = None
    for drop in ("D0", "D1", "D2"):
        for i, name in enumerate(("TransactionTypes", "TransactionTypes_Archive")):
            path = ROOT / "inputs" / drop / f"WideWorldImporters.Application.{name}.csv"
            deleted = path.with_suffix(".deleted.csv")
            state[i] = replay(state[i], csv_rows(path), csv_rows(deleted) if deleted.exists() else [], archive=bool(i))
        last, new = window(manifest, drop)
        assert last == INITIAL_CUTOFF if drop == "D0" else last != INITIAL_CUTOFF
        actual = extract(state[0] + state[1], last, new)
        current = csv_rows(ROOT / "outputs" / drop / "tables/Dimension.Transaction_Type.csv")
        expected = expected_rows(current, previous)
        assert_parity(actual, expected)
        assert len(actual) == (14 if drop == "D0" else 0)
        if drop == "D0":
            assert actual == [tuple(row[c] for c in COLUMNS) for row in current if row["transaction_type_key"] != "0"]
        previous = current
