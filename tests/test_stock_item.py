import ast
import csv
import json
import re
import sqlite3
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from stock_item.model import COLUMNS, RULES, SOURCES, TYPES, changes_sql, datetime7, window

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "golden"


def test_pipeline_loads_without_file_global(monkeypatch):
    decorator = lambda *args, **kwargs: lambda function: function
    monkeypatch.setitem(sys.modules, "pyspark", SimpleNamespace(
        pipelines=SimpleNamespace(materialized_view=decorator, expect_all=decorator)))
    source_path = str(ROOT / "src/stock_item")
    config = {"s05.source_path": source_path, "s05.catalog": "daiwt_02_mig_dev",
              "s05.bronze_schema": "bronze_pr_16"}
    monkeypatch.setattr(sys, "path", list(sys.path))
    namespace = {"spark": SimpleNamespace(conf=SimpleNamespace(get=config.__getitem__))}
    exec(compile((ROOT / "src/stock_item/pipeline.py").read_text(), "<pipeline>", "exec"), namespace)
    assert namespace["bronze"] == "`daiwt_02_mig_dev`.`bronze_pr_16`"
    assert callable(namespace["stock_item_changes"])
    assert callable(namespace["stock_item_changes_quarantine"])


@pytest.mark.parametrize("catalog,allowed", [
    ("daiwt_02_mig_dev", True), ("daiwt_02_mig_prod", True), ("other", False),
])
def test_bronze_catalog_guard(catalog, allowed):
    tree = ast.parse((ROOT / "src/stock_item/bronze_load.py").read_text())
    guard = next(node for node in tree.body if isinstance(node, ast.If)
                 and isinstance(node.test, ast.Compare)
                 and isinstance(node.test.left, ast.Name) and node.test.left.id == "catalog")
    code = compile(ast.Module(body=[guard], type_ignores=[]), "<catalog_guard>", "exec")
    if allowed:
        exec(code, {"catalog": catalog})
    else:
        with pytest.raises(ValueError):
            exec(code, {"catalog": catalog})


def rows(path):
    with path.open(newline="") as handle:
        return [{k: None if v == r"\N" else v for k, v in row.items()}
                for row in csv.DictReader(handle)]


def normalized(row):
    result = []
    for column, kind in zip(COLUMNS, TYPES):
        value = row[column]
        if value is not None:
            if kind.startswith("DECIMAL"):
                value = Decimal(str(value))
            elif kind in ("INT", "BOOLEAN"):
                value = int(value)
            elif kind == "BINARY" and isinstance(value, str):
                value = bytes.fromhex(value)
        result.append(value)
    return tuple(result)


@pytest.mark.parametrize("drop,expected", [
    ("D0", ("1900-01-01 00:00:00.0000000", "2016-06-06 12:31:17.0000000")),
    ("D1", ("2016-06-06 12:31:17.0000000", "2016-06-14 00:00:00.0000000")),
    ("D2", ("2016-06-14 00:00:00.0000000", "2016-06-21 00:00:00.0000000")),
])
def test_windows(drop, expected):
    assert window(json.loads((GOLDEN / f"capture-{drop}.json").read_text()), drop) == expected


@pytest.mark.parametrize("value", ["", None, "2016-01-01", "2016-01-01 00:00:00.12345678"])
def test_invalid_datetime(value):
    with pytest.raises(ValueError):
        datetime7(value)


def test_invalid_window():
    with pytest.raises(ValueError):
        window({}, "D3")
    with pytest.raises(ValueError):
        window({"drop": {"last_cutoff": "2016-01-01 00:00:00",
                         "new_cutoff": "2016-01-01 00:00:00"}}, "D1")


def test_registry_and_job():
    import yaml
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    entry = registry["wwi_stock_item_to_silver"]
    assert entry["outputs"] == entry["depends_on"] == []
    assert set(entry["owns"]) == {
        *("bronze." + table for table, _ in SOURCES.values()),
        "bronze.stock_item_window", "silver.stock_item_changes",
        "silver.stock_item_changes_quarantine",
    }
    resource = yaml.safe_load((ROOT / "resources/stock_item.yml").read_text())
    tasks = resource["resources"]["jobs"][entry["job"]]["tasks"]
    assert [t["task_key"] for t in tasks] == [
        f"{step}_{drop}" for drop in ("D0", "D1", "D2")
        for step in ("bronze_load", "refresh", "parity")
    ]
    for prior, task in zip(tasks, tasks[1:]):
        assert task["depends_on"] == [{"task_key": prior["task_key"]}]


def connection():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.create_function("unhex", 1, lambda value: bytes.fromhex(value) if value is not None else None)
    db.execute("ATTACH DATABASE ':memory:' AS bronze")
    for source, (table, _) in SOURCES.items():
        path = GOLDEN / "inputs/D0" / f"WideWorldImporters.Warehouse.{source}.csv"
        data = rows(path)
        with path.open() as handle:
            columns = next(csv.reader(handle))
        db.execute(f"CREATE TABLE bronze.{table} (" + ", ".join(f'"{c}" TEXT' for c in columns) + ")")
        db.executemany(f"INSERT INTO bronze.{table} VALUES (" + ",".join("?" for _ in columns) + ")",
                       [[r[c] for c in columns] for r in data])
    db.execute("CREATE TABLE bronze.stock_item_window (drop_name TEXT, last_cutoff TEXT, new_cutoff TEXT)")
    return db


def test_golden_reconstruction_and_empty_incremental():
    db = connection()
    for drop, count in (("D0", 671), ("D1", 0), ("D2", 0)):
        if drop != "D0":
            # These six source deltas and their deletions are header-only for D1/D2.
            for source in SOURCES:
                path = GOLDEN / "inputs" / drop / f"WideWorldImporters.Warehouse.{source}.csv"
                assert rows(path) == []
                deleted = path.with_name(path.stem + ".deleted.csv")
                assert not deleted.exists() or rows(deleted) == []
        last, new = window(json.loads((GOLDEN / f"capture-{drop}.json").read_text()), drop)
        db.execute("DELETE FROM bronze.stock_item_window")
        db.execute("INSERT INTO bronze.stock_item_window VALUES (?, ?, ?)", (drop, last, new))
        actual = list(db.execute(changes_sql("bronze")))
        current = rows(GOLDEN / "outputs" / drop / "tables/Dimension.Stock_Item.csv")
        previous = (rows(GOLDEN / "outputs" / f"D{int(drop[1:]) - 1}" / "tables/Dimension.Stock_Item.csv")
                    if drop != "D0" else [])
        keys = {r["stock_item_key"] for r in previous}
        expected = [r for r in current if r["stock_item_key"] != "0" and r["stock_item_key"] not in keys]
        assert len(actual) == len(expected) == count
        actual_rows = list(map(normalized, actual))
        expected_rows = list(map(normalized, expected))
        parity_sql = (ROOT / "src/stock_item/parity.sql").read_text()
        exclusions = re.search(r"THEN array\(([^)]*)\)", parity_sql).group(1)
        d0_excluded_columns = re.findall(r"'([^']+)'", exclusions)
        assert d0_excluded_columns == ["tax_rate"]
        if drop == "D0":
            differing_columns = {
                column for i, column in enumerate(COLUMNS)
                if Counter(row[i] for row in actual_rows) != Counter(row[i] for row in expected_rows)
            }
            assert differing_columns == set(d0_excluded_columns)
        excluded = set(d0_excluded_columns) if drop == "D0" else set()
        def comparable(row):
            return tuple(value for column, value in zip(COLUMNS, row) if column not in excluded)
        assert Counter(map(comparable, actual_rows)) == Counter(map(comparable, expected_rows))
        db.execute("CREATE TEMP VIEW staged AS " + changes_sql("bronze"))
        invalid = " OR ".join(f"NOT COALESCE(({rule}), FALSE)" for rule in RULES.values())
        assert db.execute(f"SELECT count(*) FROM staged WHERE {invalid}").fetchone()[0] == 0
        db.execute("DROP VIEW staged")


def test_same_time_events_do_not_close_each_other():
    db = connection()
    db.execute("INSERT INTO bronze.stock_item_window VALUES ('D0', '1900', '9999')")
    db.execute("INSERT INTO bronze.warehouse_stock_items_archive SELECT * FROM bronze.warehouse_stock_items_archive LIMIT 1")
    actual = list(db.execute(changes_sql("bronze")))
    assert all(r["valid_from"] < r["valid_to"] for r in actual)
    assert len(actual) == 674


def test_invalid_rows_quarantined_without_dropping():
    db = connection()
    db.execute("INSERT INTO bronze.stock_item_window VALUES ('D0', '1900', '9999')")
    db.execute("UPDATE bronze.warehouse_stock_items SET TaxRate = '-1'")
    actual = list(db.execute(changes_sql("bronze")))
    assert len(actual) == 671
    assert sum(r["tax_rate"] < 0 for r in actual) == 227
