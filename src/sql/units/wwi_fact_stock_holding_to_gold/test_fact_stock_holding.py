import csv
import json
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from classify_changes import classify
from s12.stock_holding_common import parse_csv, replay
from sql.units.p5_gold_seed.integration import ENTITIES
from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_fact_stock_holding_to_gold.job import run, snapshot_select
from sql.units.wwi_fact_stock_holding_to_gold.verify_stock_holding import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.fact_stock_holding" and e["drop"] == "D0")
ARGS = dict(catalog="dev", gold_schema="gold_pr_0", silver_schema="silver_pr_0",
            golden_path="/golden", last_cutoff="2016-06-06 12:31:17.0000000",
            new_cutoff="2016-06-14 00:00:00.0000000")


def rows(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self, present=True, fail=False, counts=(227, 227, 0, 0, 0)):
        self.statements, self.present, self.fail, self.counts = [], present, fail, counts

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT DISTINCT"):
            return Result([("D1",)] if self.present else [])
        if statement.startswith("SELECT lineage_key"):
            return Result([(i + 14, name) for i, name in enumerate(ENTITIES)])
        if statement.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        if "SELECT (SELECT count(*) FROM golden)" in statement:
            return Result([self.counts])
        if self.fail and statement.startswith("CREATE OR REPLACE TABLE"):
            raise RuntimeError("write failed")
        return Result()


def test_d0_seed():
    spark = Spark(present=False)
    run(spark, CONTRACT, **ARGS, drop="D0")
    assert len(spark.statements) == 1
    assert "/outputs/D0/tables/Fact.Stock_Holding.csv" in spark.statements[0]
    assert "CREATE OR REPLACE TABLE" in spark.statements[0]
    assert "CAST(`last_cost_price` AS DECIMAL(18,2))" in spark.statements[0]


@pytest.mark.parametrize("drop", ["D1", "D2"])
def test_reload_and_complete(drop):
    spark = Spark()
    run(spark, CONTRACT, **ARGS, drop=drop)
    sql = "\n".join(spark.statements)
    assert f"WHERE s.`drop` = '{drop}'" in sql
    assert "ORDER BY s.wwi_stock_item_id" in sql
    assert "ORDER BY valid_to DESC, stock_item_key DESC" in sql
    assert "CAST(CAST(s.last_cost_price AS STRING) AS DECIMAL(18,2))" in sql
    assert "coalesce(d.stock_item_key, 0)" in sql
    assert "CAST(25 AS BIGINT)" in sql
    assert spark.statements[-1].startswith("UPDATE")
    assert "WHERE lineage_key = 25" in spark.statements[-1]
    assert "valid_from >" not in sql


def test_guard_and_failed_write():
    spark = Spark(present=False)
    with pytest.raises(ValueError, match="silver for this drop"):
        run(spark, CONTRACT, **ARGS, drop="D2")
    assert len(spark.statements) == 1
    spark = Spark(fail=True)
    with pytest.raises(RuntimeError, match="write failed"):
        run(spark, CONTRACT, **ARGS, drop="D1")
    assert not any(s.startswith("UPDATE") for s in spark.statements)


@pytest.mark.parametrize("override", [dict(drop="D3"), dict(last_cutoff="bad"),
                                     dict(last_cutoff="2017-01-01 00:00:00.0000000")])
def test_invalid_window(override):
    spark = Spark()
    with pytest.raises(ValueError):
        run(spark, CONTRACT, **(ARGS | dict(drop="D1") | override))
    assert spark.statements == []


def normalized(row):
    return tuple(None if row[c] is None else
                 str(row[c]) if c == "bin_location" else Decimal(str(row[c]))
                 for c in ENTRY["columns"])


def test_executed_query_all_golden_rows_and_key_order():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("CREATE TABLE snapshot (wwi_stock_item_id INT, quantity_on_hand INT, bin_location TEXT, "
               "last_stocktake_quantity INT, last_cost_price TEXT, reorder_level INT, target_stock_level INT, `drop` TEXT)")
    db.execute("CREATE TABLE dimension (stock_item_key INT, wwi_stock_item_id INT, valid_to TEXT)")
    state = {}
    for index in range(3):
        drop = f"D{index}"
        path = ROOT / f"golden/inputs/{drop}/WideWorldImporters.Warehouse.StockItemHoldings.csv"
        deleted = path.with_name(path.stem + ".deleted.csv")
        state = replay(state, parse_csv(path.read_text()),
                       parse_csv(deleted.read_text(), deleted=True) if deleted.exists() else [])
        db.executemany("INSERT INTO snapshot VALUES (?,?,?,?,?,?,?,?)", [
            [r[c] for c in ("StockItemID", "QuantityOnHand", "BinLocation", "LastStocktakeQuantity",
                           "LastCostPrice", "ReorderLevel", "TargetStockLevel")] + [drop]
            for r in state.values()])
        dimension = rows(ROOT / f"golden/outputs/{drop}/tables/Dimension.Stock_Item.csv")
        db.execute("DELETE FROM dimension")
        db.executemany("INSERT INTO dimension VALUES (?,?,?)",
                       [(r["stock_item_key"], r["wwi_stock_item_id"], r["valid_to"]) for r in dimension])
        expected = rows(ROOT / f"golden/outputs/{drop}/tables/Fact.Stock_Holding.csv")
        ids = {r["stock_item_key"]: int(r["wwi_stock_item_id"]) for r in dimension}
        assert [ids[r["stock_item_key"]] for r in expected] == list(range(1, 228))
        if drop == "D0":
            assert len(expected) == 227
            continue
        query = snapshot_select(ENTRY, "snapshot", "dimension", drop, 12 + index * 13)
        actual = list(db.execute(query.replace(" AS STRING", " AS TEXT")))
        assert len(actual) == len(expected) == 227
        assert sorted(map(normalized, actual)) == sorted(map(normalized, expected))


def test_executed_empty_and_missing_dimension():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE snapshot (wwi_stock_item_id INT, quantity_on_hand INT, bin_location TEXT, "
               "last_stocktake_quantity INT, last_cost_price TEXT, reorder_level INT, target_stock_level INT, `drop` TEXT)")
    db.execute("CREATE TABLE dimension (stock_item_key INT, wwi_stock_item_id INT, valid_to TEXT)")
    query = snapshot_select(ENTRY, "snapshot", "dimension", "D1", 25).replace(" AS STRING", " AS TEXT")
    assert list(db.execute(query)) == []
    db.execute("INSERT INTO snapshot VALUES (99,1,'A',2,'9.50',3,4,'D1')")
    assert list(db.execute(query))[0] == (1, 0, 1, "A", 2, 9.5, 3, 4, 25)


@pytest.mark.parametrize("counts,status", [((227, 227, 0, 0, 0), "PASS"),
                                           ((227, 227, 1, 1, 0), "FAIL")])
def test_parity(counts, status):
    spark = Spark(counts=counts)
    if status == "PASS":
        assert parity(spark, CONTRACT, catalog="dev", gold_schema="gold_pr_0",
                      golden_path="/golden", drop="D1")["status"] == status
    else:
        with pytest.raises(RuntimeError, match="parity failed"):
            parity(spark, CONTRACT, catalog="dev", gold_schema="gold_pr_0",
                   golden_path="/golden", drop="D1")


def test_job_and_registry():
    unit = "wwi_fact_stock_holding_to_gold"
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry[unit] == dict(job=unit, outputs=["gold.fact_stock_holding"], owns=[],
                                 depends_on=["p5_gold_seed", "wwi_stock_holding_to_silver", "wwi_dim_stock_item_to_gold"])
    assert unit in classify(registry, [f"resources/{unit}.job.yml"])
    job = yaml.safe_load((ROOT / f"resources/{unit}.job.yml").read_text())["resources"]["jobs"][unit]
    assert "permissions" not in job
    assert [t["task_key"] for t in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "load"}]
