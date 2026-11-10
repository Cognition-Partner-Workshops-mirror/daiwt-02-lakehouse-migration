import copy
import json
import re
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from parity.parity_check import (
    ROOT, Warehouse, identifier, normalize, render_column, run, select_entries,
)

CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
CATALOG = "daiwt_02_mig_dev"
TABLE = "gold.integration_lineage"
ENTRY = next(e for e in CONTRACT["entries"] if e["drop"] == "D2" and e["table"] == TABLE)
REGISTRY = {"framework": {"outputs": [TABLE]}}


class FakeWarehouse:
    def __init__(self, counts=None, error=None):
        self.statements = []
        self.counts = counts or [ENTRY["row_count"], ENTRY["row_count"], 0, 0, 0]
        self.error = error

    def sql(self, sql):
        self.statements.append(sql)
        if self.error:
            raise RuntimeError(self.error)
        if sql.startswith("DESCRIBE"):
            return [[c, "STRING"] for c in ENTRY["columns"]]
        if sql.endswith("LIMIT 10"):
            assert "m.`lineage_key` <=> e.`lineage_key`" in sql
            return [["27", "table_name", "City", "Changed"]]
        if sql.endswith("LIMIT 5"):
            assert "LEFT ANTI JOIN" in sql
            row = ["27", "2026-01-01 00:00:00.0000000", "City", "<ts>", "1", "cutoff"]
            if "SELECT e.* FROM extra" in sql:
                row[2] = "Changed"
            return [row]
        return [self.counts]


def execute(registry=REGISTRY, warehouse=None, **kwargs):
    return run(None, CATALOG, "pr_1", "w", registry, warehouse=warehouse or FakeWarehouse(), **kwargs)


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_drop_selection(drop):
    entries = select_entries(CONTRACT, drop)
    assert len(entries) == 18
    assert {e["table"] for e in entries} == set(CONTRACT["outputs"])
    assert all(e["drop"] == drop for e in entries)


def test_bad_contract_and_drop():
    with pytest.raises(ValueError):
        select_entries(CONTRACT, "D3")
    bad = copy.deepcopy(CONTRACT)
    bad["entries"].pop()
    with pytest.raises(ValueError):
        select_entries(bad, "D2")
    bad = copy.deepcopy(CONTRACT)
    bad["entries"][-1] = bad["entries"][-2]
    with pytest.raises(ValueError):
        select_entries(bad, "D2")


def test_all_skip_no_sql():
    warehouse = FakeWarehouse(error="must not query")
    result = execute({}, warehouse)
    assert result["status"] == "PASS"
    assert result["counts"] == {"PASS": 0, "FAIL": 0, "SKIP": 18}
    assert result["compared"] == 0
    assert not warehouse.statements
    assert all(r["reason"] == "not yet migrated" for r in result["results"])


def test_registered_pass_and_sql():
    warehouse = FakeWarehouse()
    result = execute(warehouse=warehouse)
    assert result["counts"] == {"PASS": 1, "FAIL": 0, "SKIP": 17}
    assert result["compared"] == 1
    sql = warehouse.statements[1]
    for part in ("read_files(", "outputs/D2/tables/Integration.Lineage.csv",
                 "EXCEPT ALL", "mode=>'FAILFAST'", "multiLine=>true", "STRING",
                 "gold_pr_1", "data_load_completed", "'<ts>'"):
        assert part in sql
    assert "LIMIT" not in sql


@pytest.mark.parametrize("error,reason", [
    ("TABLE_OR_VIEW_NOT_FOUND", "missing table"), ("permission denied", "permission denied"),
])
def test_registered_exception(error, reason):
    result = execute(warehouse=FakeWarehouse(error=error))
    assert result["status"] == "FAIL"
    row = next(r for r in result["results"] if r["table"] == TABLE)
    assert reason in row["reason"]
    assert result["compared"] == 1


def test_diff_and_contract_count():
    warehouse = FakeWarehouse([ENTRY["row_count"], ENTRY["row_count"], 1, 1, 0])
    result = execute(warehouse=warehouse)
    row = next(r for r in result["results"] if r["table"] == TABLE)
    assert row["status"] == "FAIL"
    assert row["diff"]["columns"] == [
        {"key": "27", "column": "table_name", "golden": "City", "actual": "Changed"}]
    assert len(row["diff"]["missing"]) + len(row["diff"]["extra"]) <= 10
    warehouse = FakeWarehouse([1, 1, 0, 0, 0])
    row = next(r for r in execute(warehouse=warehouse)["results"] if r["table"] == TABLE)
    assert row["reason"] == "golden file differs from contract"


def test_invalid_timestamps_fail_even_when_identically_invalid():
    warehouse = FakeWarehouse([ENTRY["row_count"], ENTRY["row_count"], 0, 0, 2])
    row = next(r for r in execute(warehouse=warehouse)["results"] if r["table"] == TABLE)
    assert row["status"] == "FAIL"
    assert row["reason"] == "invalid normalized timestamp"


def test_registered_empty_tables_and_prod_schema():
    contract = copy.deepcopy(CONTRACT)
    entry = next(e for e in contract["entries"] if e["table"] == TABLE and e["drop"] == "D2")
    entry["row_count"] = 0
    warehouse = FakeWarehouse([0, 0, 0, 0, 0])
    result = run(None, "daiwt_02_mig_prod", "prod", "w", REGISTRY,
                 warehouse=warehouse, contract=contract)
    assert result["counts"] == {"PASS": 1, "FAIL": 0, "SKIP": 17}
    assert "`daiwt_02_mig_prod`.`gold`.`integration_lineage`" in warehouse.statements[0]
    warehouse = FakeWarehouse([ENTRY["row_count"], 0, ENTRY["row_count"], 0, 0])
    assert execute(warehouse=warehouse)["status"] == "FAIL"


def test_normalizer_scope_and_predicates():
    expression = normalize(ENTRY["columns"], ENTRY["compare"])
    assert expression.count("THEN '<ts>'") == 2
    assert "`data_load_completed` >= `data_load_started`" in expression
    assert "`data_load_completed` IS NOT NULL" in expression
    assert "'<invalid>'" in expression
    assert expression.startswith("`lineage_key`,")
    dim = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_payment_method")
    assert normalize(dim["columns"], dim["compare"]) == ", ".join(identifier(c) for c in dim["columns"])
    with pytest.raises(ValueError, match="Unknown comparator"):
        normalize(["id"], {"id": "ignore"})


@pytest.mark.parametrize("column,value,matches", [
    ("data_load_started", "2017-01-01 00:00:00.0000000", True),
    ("data_load_completed", None, False),
    ("data_load_completed", "2015-01-01 00:00:00.0000000", False),
    ("data_load_completed", "not a timestamp", False),
    ("lineage_key", "28", False),
])
def test_normalizer_values(column, value, matches):
    columns = ENTRY["columns"]
    golden = dict(zip(columns, ["27", "2016-01-01 00:00:00.0000000", "City",
                               "2018-01-01 00:00:00.0000000", "1", "cutoff"]))
    actual = {**golden, column: value}
    db = sqlite3.connect(":memory:")
    db.create_function("regexp", 2, lambda pattern, text: bool(text and re.search(pattern, text)))
    expression = normalize(columns, ENTRY["compare"]).replace(" RLIKE ", " REGEXP ")
    # SQLite string literals do not interpret Spark SQL backslash escapes.
    expression = expression.replace("\\\\.", "\\.")
    def normalized(row):
        select = ", ".join(f"? AS {identifier(c)}" for c in columns)
        return db.execute(f"SELECT {expression} FROM (SELECT {select})", list(row.values())).fetchone()
    assert (normalized(golden) == normalized(actual)) is matches
    db.close()


def test_valid_to_is_strict():
    expression = normalize(["id", "valid_to"], {"id": "strict", "valid_to": "strict"})
    db = sqlite3.connect(":memory:")
    assert db.execute(f"SELECT {expression} FROM (SELECT '1' AS id, '9999' AS valid_to)").fetchone() != (
        db.execute(f"SELECT {expression} FROM (SELECT '1' AS id, '9998' AS valid_to)").fetchone())
    db.close()


@pytest.mark.parametrize("kind,fragment", [
    ("STRING", "`c`"), ("TIMESTAMP", "SSSSSS"), ("TIMESTAMP_NTZ", "'0'"),
    ("DATE", "yyyy-MM-dd"), ("DECIMAL(18,4)", "CAST(`c` AS STRING)"),
    ("BOOLEAN", "THEN '1' ELSE '0'"), ("BINARY", "upper(hex("),
    ("INT", "CAST("), ("BIGINT", "CAST("),
])
def test_type_rendering(kind, fragment):
    assert fragment in render_column("c", kind)


def test_invalid_inputs():
    with pytest.raises(ValueError):
        identifier("bad;sql")
    with pytest.raises(ValueError):
        render_column("c", "ARRAY<STRING>")
    for suffix in ("prod", "pr_x", "pr_1;"):
        with pytest.raises(ValueError):
            run(None, CATALOG, suffix, "w", {})
    with pytest.raises(ValueError, match="Multiple producers"):
        execute({"a": REGISTRY["framework"], "b": REGISTRY["framework"]})


def test_cli_required_suffix_and_drop():
    path = str(ROOT / "src/parity/parity_check.py")
    assert subprocess.run([sys.executable, path], capture_output=True).returncode == 2
    assert subprocess.run([sys.executable, path, "--suffix", "pr_1", "--drop", "D3"],
                          capture_output=True).returncode == 2


def test_sql_failure():
    response = SimpleNamespace(status=SimpleNamespace(state=SimpleNamespace(value="FAILED"), error="bad SQL"))
    client = SimpleNamespace(statement_execution=SimpleNamespace(execute_statement=lambda **_: response))
    with pytest.raises(RuntimeError, match="bad SQL"):
        Warehouse(client, "w").sql("SELECT 1")


def test_chunked_result():
    response = SimpleNamespace(
        status=SimpleNamespace(state=SimpleNamespace(value="SUCCEEDED")),
        statement_id="s", result=SimpleNamespace(data_array=[["1"]], next_chunk_index=1),
        manifest=SimpleNamespace(truncated=False))
    client = SimpleNamespace(statement_execution=SimpleNamespace(
        execute_statement=lambda **_: response,
        get_statement_result_chunk_n=lambda *_: SimpleNamespace(data_array=[["2"]], next_chunk_index=None)))
    assert Warehouse(client, "w").sql("SELECT 1") == [["1"], ["2"]]
