import csv
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

from classify_changes import classify, expand
from parity.parity_check import run
from s12.stock_holding_common import (
    PARITY, SOURCE, compare_rows, drops_through, parity_price, parse_csv, qualified, replay,
    snapshot_query, verification_drops, RULES,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def p4_db():
    with sqlite3.connect(":memory:") as db:
        db.row_factory = sqlite3.Row
        db.execute("CREATE TABLE raw (" + ",".join(f"{c} TEXT" for c in SOURCE) +
                   ", _drop TEXT, _op TEXT)")
        db.execute("CREATE TABLE replay_log (`drop` TEXT, source_table TEXT, status TEXT)")
        yield db


def local_snapshot_query():
    # SQLite casts exercise replay windows over valid, castable P4 source values.
    return snapshot_query("raw", "replay_log").replace("try_cast(", "cast(").replace(" AS STRING", " AS TEXT")


def complete_drop(db, drop):
    db.execute("INSERT INTO replay_log VALUES (?, 'Warehouse.StockItemHoldings', 'PASS')", (drop,))


def test_drop_validation():
    assert drops_through("D0") == ["D0"]
    assert drops_through("D2") == ["D0", "D1", "D2"]
    for drop in ("D3", "d0", "", "D0'", None):
        with pytest.raises(ValueError):
            drops_through(drop)


def test_requested_drop_verification_after_d2_landed():
    for drop in drops_through("D2"):
        assert verification_drops(["D0", "D1", "D2"], drop) == drops_through(drop)
    with pytest.raises(ValueError, match="Missing requested drops"):
        verification_drops(["D0", "D2"], "D2")
    with pytest.raises(ValueError):
        verification_drops(["D0"], "D0' OR TRUE")


def test_csv_null_header_only_and_errors():
    header = ",".join(SOURCE)
    assert parse_csv(header + "\n") == []
    row = parse_csv(header + "\n1,2,\\N,3,9.50,4,5,6,2016-01-01\n")[0]
    assert row["BinLocation"] is None
    assert row["LastCostPrice"] == "9.50"
    assert parse_csv("StockItemID\n1\n", deleted=True) == [{"StockItemID": "1"}]
    for content in ("bad\n", header + "\n1,2\n", header + "\n" + ",".join(["1"] * 10)):
        with pytest.raises(ValueError):
            parse_csv(content)


def test_replay_delete_then_upsert_and_idempotence():
    old = {"StockItemID": "1", "LastCostPrice": "9.50"}
    new = {"StockItemID": "1", "LastCostPrice": "11.25"}
    other = {"StockItemID": "2"}
    initial = replay({}, [old, other])
    assert replay(initial, []) == initial
    assert replay(initial, [], [old]) == {"2": other}
    updated = replay(initial, [new], [old, other])
    assert updated == {"1": new}
    assert replay(updated, [new], [old, other]) == updated
    assert initial == {"1": old, "2": other}
    assert replay({}, [], [old]) == {}


@pytest.mark.parametrize("value,baseline,other", [
    ("9.50", "9.00", "9.50"), ("-9.50", "-9.00", "-9.50"),
    ("0.99", "0.00", "0.99"), ("-0.99", "0.00", "-0.99"),
    ("11.25", "11.00", "11.25"), ("10.00", "10.00", "10.00"),
])
def test_d0_native_result_type_only(value, baseline, other):
    assert parity_price(Decimal(value), "D0") == baseline
    assert parity_price(Decimal(value), "D1") == other
    assert parity_price(Decimal(value), "D2") == other
    assert parity_price(None, "D0") is None


def test_exact_multiset_comparison():
    assert compare_rows([], []) == dict(actual_rows=0, golden_rows=0, actual_only=0, golden_only=0)
    assert compare_rows([["1"], ["2"]], [["2"], ["1"]])["actual_only"] == 0
    assert compare_rows([["1"], ["1"]], [["1"]])["actual_only"] == 1
    diff = compare_rows([["1", "9.50"]], [["1", "9.00"]])
    assert diff["actual_only"] == diff["golden_only"] == 1
    assert compare_rows([[None]], [["\\N"]])["actual_only"] == 1


def test_all_golden_snapshots_exact_with_d0_native_type(p4_db):
    state = {}
    for drop in drops_through("D2"):
        folder = ROOT / "golden" / "inputs" / drop
        rows = parse_csv((folder / "WideWorldImporters.Warehouse.StockItemHoldings.csv").read_text())
        deleted = folder / "WideWorldImporters.Warehouse.StockItemHoldings.deleted.csv"
        deletes = parse_csv(deleted.read_text(), deleted=True) if deleted.exists() else []
        state = replay(state, rows, deletes)
        for op, source_rows in [("delete", deletes), ("base" if drop == "D0" else "upsert", rows)]:
            p4_db.executemany("INSERT INTO raw VALUES (" + ",".join("?" * 11) + ")",
                              [tuple(r.get(c) for c in SOURCE) + (drop, op) for r in source_rows])
        complete_drop(p4_db, drop)
        outputs = ROOT / "golden" / "outputs" / drop / "tables"
        with (outputs / "Dimension.Stock_Item.csv").open() as stream:
            mapping = {r["stock_item_key"]: r["wwi_stock_item_id"] for r in csv.DictReader(stream)}
        with (outputs / "Fact.Stock_Holding.csv").open() as stream:
            facts = list(csv.DictReader(stream))
        expected = [[mapping[r["stock_item_key"]]] + [r[c] for c in PARITY[1:]] for r in facts]
        actual = []
        for row in state.values():
            projected = {dst: row[src] for src, (dst, _) in SOURCE.items()}
            projected["last_cost_price"] = parity_price(projected["last_cost_price"], drop)
            actual.append([projected[c] for c in PARITY])
            assert row["StockItemID"] is not None
            assert row["BinLocation"] is not None and len(row["BinLocation"]) <= 20
            for col in ("QuantityOnHand", "LastStocktakeQuantity", "LastCostPrice", "ReorderLevel", "TargetStockLevel"):
                assert Decimal(row[col]) >= 0
        assert compare_rows(actual, expected) == dict(actual_rows=227, golden_rows=227, actual_only=0, golden_only=0)
        assert state["1"]["LastCostPrice"] == "9.50"
        queried = []
        for row in p4_db.execute(f"SELECT * FROM ({local_snapshot_query()}) WHERE `drop` = ?", (drop,)):
            projected = {c: str(row[c]) if row[c] is not None else None for c in PARITY}
            projected["last_cost_price"] = parity_price(row["last_cost_price"], drop)
            queried.append([projected[c] for c in PARITY])
        assert compare_rows(queried, expected) == dict(actual_rows=227, golden_rows=227, actual_only=0, golden_only=0)
    assert len(list(p4_db.execute(local_snapshot_query()))) == 3 * 227


def test_registry_and_classification():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    unit = "wwi_stock_holding_to_silver"
    assert registry[unit]["outputs"] == []
    assert registry[unit]["depends_on"] == ["bronze_landing_replay"]
    assert registry[unit]["owns"] == ["silver.stock_holding_snapshot", "silver.stock_holding_snapshot_quarantine", "silver.stock_holding_parity"]
    expanded = expand(registry, ["bronze_landing_replay"])
    assert unit in expanded
    assert expanded.index("bronze_landing_replay") < expanded.index(unit)
    for path in ("src/s12/stock_holding_common.py",
                 "src/s12/stock_holding_pipeline.py", "src/s12/verify_stock_holding.py",
                 "resources/stock_holding.yml"):
        assert unit in classify(registry, [path])
    assert run(None, "daiwt_02_mig_dev", "pr_15", "unused", registry)["status"] == "PASS"


def test_query_uses_complete_drops_and_safe_identifiers():
    table = qualified("catalog", "bronze", "warehouse_stock_item_holdings_raw")
    query = snapshot_query(table, qualified("catalog", "bronze", "bronze_replay_log"))
    assert "DECIMAL(18,2)" in query
    assert "status = 'PASS'" in query
    assert "_op = 'delete'" in query
    assert "source_table = 'Warehouse.StockItemHoldings'" in query
    assert "l._drop <= d._drop" in query
    with pytest.raises(ValueError):
        qualified("catalog; DROP", "bronze", "landing")


def test_duplicate_source_rows_survive_ranking_and_fail_uniqueness(p4_db):
    values = [
        ("1", "10", "A", "10", "9.50", "1", "20", "1", "2016-01-01", "D0", "base"),
        ("1", "11", "B", "11", "9.50", "1", "20", "1", "2016-01-01", "D0", "base"),
        ("1", "12", "C", "12", "9.50", "1", "20", "1", "2016-01-01", "D1", "upsert"),
    ]
    p4_db.executemany("INSERT INTO raw VALUES (" + ",".join("?" * 11) + ")", values)
    complete_drop(p4_db, "D0")
    complete_drop(p4_db, "D1")
    query = local_snapshot_query()
    checked = f"SELECT *, count(*) OVER (PARTITION BY `drop`, wwi_stock_item_id) AS _key_count FROM ({query})"
    rows = list(p4_db.execute(checked))
    assert len(rows) == 3
    assert [(r["quantity_on_hand"], r["_source_key_count"]) for r in rows if r["drop"] == "D0"] == [(10, 2), (11, 2)]
    quarantined = list(p4_db.execute(f"SELECT * FROM ({checked}) WHERE NOT ({RULES['unique_drop_id']})"))
    assert len(quarantined) == 2
    assert all(r["drop"] == "D0" for r in quarantined)
    assert p4_db.execute("SELECT count(*) FROM raw").fetchone()[0] == len(values)
    p4_db.execute("DELETE FROM raw")
    assert list(p4_db.execute(query)) == []


def test_p4_delete_upsert_header_only_and_failed_drop(p4_db):
    row = ("1", "10", "A", "10", "9.50", "1", "20", "1", "2016-01-01")
    deleted = ("1",) + (None,) * 8
    p4_db.executemany("INSERT INTO raw VALUES (" + ",".join("?" * 11) + ")", [
        row + ("D0", "base"), deleted + ("D1", "delete"), row + ("D1", "upsert"),
        deleted + ("D2", "delete"),
    ])
    complete_drop(p4_db, "D0")
    complete_drop(p4_db, "D1")
    p4_db.execute("INSERT INTO replay_log VALUES ('D2', 'Warehouse.StockItemHoldings', 'FAIL')")
    rows = list(p4_db.execute(local_snapshot_query()))
    assert [r["drop"] for r in rows] == ["D0", "D1"]
    assert all(r["_source_key_count"] == 1 for r in rows)
    complete_drop(p4_db, "D2")
    assert len(list(p4_db.execute(local_snapshot_query()))) == 2
    p4_db.execute("DELETE FROM raw WHERE _drop IN ('D1', 'D2')")
    assert [r["drop"] for r in p4_db.execute(local_snapshot_query())] == ["D0", "D1", "D2"]
