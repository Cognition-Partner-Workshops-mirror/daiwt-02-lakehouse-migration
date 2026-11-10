import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path

import pytest
import yaml

from classify_changes import classify
from sql.units.wwi_report_customer_history_to_gold.job import COLUMNS, report_sql, run
from sql.units.wwi_report_customer_history_to_gold.verify import run as verify

ROOT = Path(__file__).resolve().parents[4]
UNIT = "wwi_report_customer_history_to_gold"
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
DIM_COLUMNS = next(e["columns"] for e in CONTRACT["entries"]
                   if e["table"] == "gold.dim_customer" and e["drop"] == "D0")
ARGS = dict(catalog="dev", gold_schema="gold")


def golden(drop, name):
    with (ROOT / f"golden/outputs/{drop}/{name}").open() as stream:
        return list(csv.reader(stream))


class Spark:
    """Run the generated report SQL against SQLite, adapting the Delta CTAS."""
    def __init__(self, rows):
        self.statements = []
        self.db = sqlite3.connect(":memory:")
        self.db.execute("ATTACH DATABASE ':memory:' AS dev")
        self.db.execute("CREATE TABLE dev.dim_customer (" + ", ".join(DIM_COLUMNS) + ")")
        self.db.executemany("INSERT INTO dev.dim_customer VALUES (" + ",".join("?" for _ in DIM_COLUMNS) + ")", rows)

    def sql(self, statement):
        self.statements.append(statement)
        prefix = "CREATE OR REPLACE TABLE `dev`.`gold`.`report_customer_history` USING DELTA AS "
        assert statement.startswith(prefix)
        self.db.execute("DROP TABLE IF EXISTS dev.report_customer_history")
        body = statement[len(prefix):].replace("`dev`.`gold`.`dim_customer`", "dev.dim_customer")
        self.db.execute("CREATE TABLE dev.report_customer_history AS " + body)

        class Result:
            def collect(self):
                return []
        return Result()


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_report_reproduces_golden_from_each_drop_dimension(drop):
    dimension = golden(drop, "tables/Dimension.Customer.csv")
    assert dimension[0] == DIM_COLUMNS
    spark = Spark(dimension[1:])
    run(spark, CONTRACT, drop=drop, **ARGS)
    expected = golden(drop, "reports/customer-history.csv")
    assert expected[0] == COLUMNS
    actual = spark.db.execute("SELECT * FROM dev.report_customer_history").fetchall()
    assert Counter(tuple(r) for r in actual) == Counter(tuple(r) for r in expected[1:])
    assert len(actual) == 403


def test_report_sql_is_unfiltered_projection_of_dim_customer():
    sql = report_sql("dev", "gold")
    assert "SELECT customer_key, wwi_customer_id, customer, valid_from, valid_to " in sql
    assert "FROM `dev`.`gold`.`dim_customer` ORDER BY customer_key" in sql
    assert "WHERE" not in sql


@pytest.mark.parametrize("drop", ["D3", ""])
def test_invalid_drop(drop):
    spark = Spark([])
    with pytest.raises(ValueError):
        run(spark, CONTRACT, drop=drop, **ARGS)
    assert spark.statements == []


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
@pytest.mark.parametrize("status", ["PASS", "FAIL"])
def test_visible_parity_task_uses_drop_contract(monkeypatch, drop, status):
    def compare(warehouse, entry, path, catalog, schema):
        assert entry["table"] == "gold.report_customer_history" and entry["drop"] == drop
        assert entry["path"] == f"outputs/{drop}/reports/customer-history.csv"
        return dict(status=status, reason="row mismatch")
    monkeypatch.setattr(f"sql.units.{UNIT}.verify.compare_entry", compare)
    kwargs = dict(ARGS, golden_path="/golden")
    if status == "PASS":
        assert verify(None, CONTRACT, drop=drop, **kwargs)["status"] == "PASS"
    else:
        with pytest.raises(RuntimeError, match="report_customer_history parity failed"):
            verify(None, CONTRACT, drop=drop, **kwargs)


def test_unit_ownership_classification_and_visible_job_tasks():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry[UNIT] == dict(job=UNIT, outputs=["gold.report_customer_history"], owns=[],
                                  depends_on=["wwi_dim_customer_to_gold"])
    assert UNIT in classify(registry, [f"resources/{UNIT}.job.yml"])
    assert UNIT in classify(registry, ["src/sql/units/wwi_dim_customer_to_gold/job.py"])
    job = yaml.safe_load((ROOT / f"resources/{UNIT}.job.yml").read_text())["resources"]["jobs"][UNIT]
    assert [task["task_key"] for task in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "load"}]
    assert {p["name"] for p in job["parameters"]} == {"drop", "last_cutoff", "new_cutoff"}
    assert "permissions" not in job
