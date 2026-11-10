import csv
import json
from datetime import datetime
from pathlib import Path

import pytest

from src.movement.logic import EXPECTED, PROJECTION, SHARED, assert_parity, in_window, project, parse_timestamp

from src.bronze.bronze_landing_replay import reference_replay

ROOT = Path(__file__).resolve().parents[1]


def rows(path):
    with path.open() as stream:
        yield from csv.DictReader(stream)


def test_native_column_order():
    import re
    native = json.loads((ROOT / "golden/native_result_sets.json").read_text())["Movement"]
    names = re.findall(r"\[([^]]+)\]", native)
    assert list(PROJECTION) == [x.lower().replace(" ", "_") for x in names]


def test_transform_null_and_truncation():
    row = next(rows(ROOT / "golden/inputs/D0/WideWorldImporters.Warehouse.StockItemTransactions.csv"))
    row.update(Quantity="-2.999", CustomerID="\\N", TransactionOccurredWhen="2016-01-01 01:02:03.1234567")
    result = project(row)
    assert result["quantity"] == -2
    assert result["wwi_customer_id"] is None
    assert result["last_modified_when"] == datetime(2016, 1, 1, 1, 2, 3, 123456)
    row["Quantity"] = "bad"
    with pytest.raises(Exception):
        project(row)
    row["Quantity"] = "2147483648"
    with pytest.raises(ValueError):
        project(row)


def test_window_boundaries():
    assert not in_window("a", "D1", "a", "b")
    assert in_window("b", "D1", "a", "b")
    assert not in_window("c", "D1", "a", "b")
    assert in_window("a", "D0", "b", "b")
    assert not in_window(None, "D0", "a", "b")


def test_parity_empty_and_errors():
    assert_parity([], [])
    assert_parity([(1,)], [(1,), (0,)], [(0,)])
    for actual, expected, count, quarantine in [([], [(1,)], None, 0),
            ([(1,), (1,)], [(1,)], None, 0), ([(2,)], [(1,)], None, 0),
            ([], [], 1, 0), ([], [], 0, 1)]:
        with pytest.raises(AssertionError):
            assert_parity(actual, expected, expected_count=count, quarantine=quarantine)


def test_golden_replay_all_drops():
    state, previous = [], []
    table = next(t for t in json.loads((ROOT / "src/bronze/tables.json").read_text())["tables"]
                 if t["source_table"] == "Warehouse.StockItemTransactions")
    columns = [c["name"] for c in table["columns"]]
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    for spec in manifest["drops"]:
        drop = spec["name"]
        path = ROOT / f"golden/inputs/{drop}/WideWorldImporters.Warehouse.StockItemTransactions.csv"
        deleted = path.with_suffix(".deleted.csv")
        upserts = [list(row.values()) for row in rows(path)]
        deletes = [[row["StockItemTransactionID"]] for row in rows(deleted)] if deleted.exists() else []
        state = reference_replay(table, [] if drop == "D0" else state, upserts, deletes)
        last = next(x["Cutoff Time"] for x in spec["last_cutoff"] if x["Table Name"] == "Movement")
        new = spec["new_cutoff"]
        if isinstance(new, list):
            new = next(x["Cutoff Time"] for x in new if x["Table Name"] == "Movement")
        last, new = parse_timestamp(last), parse_timestamp(new)
        staged = []
        for values in state:
            row = dict(zip(columns, values))
            if in_window(parse_timestamp(row["LastEditedWhen"]), drop, last, new):
                value = project(row)
                assert all(value[c] is not None for c in (
                    "wwi_stock_item_transaction_id", "wwi_stock_item_id",
                    "wwi_transaction_type_id", "quantity", "date_key"))
                staged.append(tuple("\\N" if value[c] is None else str(value[c]) for c in SHARED))
        current = [tuple(row[c] for c in SHARED) for row in rows(ROOT / f"golden/outputs/{drop}/tables/Fact.Movement.csv")]
        assert_parity(staged, current, previous, EXPECTED[drop])
        print(f"Movement {drop}: PASS rows={len(staged)}, missing=0, extra=0, quarantine=0")
        previous = current
