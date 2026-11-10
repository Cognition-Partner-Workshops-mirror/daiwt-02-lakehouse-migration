import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path

import pytest
import yaml

from sql.framework import END_OF_TIME, START_OF_TIME
from sql.units.wwi_dim_customer_to_gold.job import run
from sql.units.wwi_dim_customer_to_gold.verify import run as verify

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
COLUMNS = next(e["columns"] for e in CONTRACT["entries"]
               if e["table"] == "gold.dim_customer" and e["drop"] == "D0")
SOURCE_COLUMNS = [c for c in COLUMNS if c not in ("customer_key", "lineage_key")]
ARGS = dict(catalog="dev", gold_schema="gold", silver_schema="silver", golden_path="/golden",
            last_cutoff="2016-06-06 12:31:17.0000000", new_cutoff="2016-06-14 00:00:00.0000000")


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    """Execute the generated insert SQL; adapt Delta seed/MERGE to SQLite."""
    def __init__(self, lineage=((15,),)):
        self.lineage = lineage
        self.statements = []
        self.db = sqlite3.connect(":memory:")
        self.db.execute("ATTACH DATABASE ':memory:' AS dev")
        self.db.execute("CREATE TABLE dev.dim_customer (" + ", ".join(COLUMNS) + ")")
        self.db.execute("CREATE TABLE dev.customer_changes (" + ", ".join(SOURCE_COLUMNS) + ")")

    def sql(self, statement):
        self.statements.append(statement)
        if "SELECT lineage_key FROM" in statement:
            return Result(self.lineage)
        if statement.startswith("CREATE OR REPLACE TABLE"):
            assert "/outputs/D0/tables/Dimension.Customer.csv" in statement
            self.db.execute("DELETE FROM dev.dim_customer")
            with (ROOT / "golden/outputs/D0/tables/Dimension.Customer.csv").open() as stream:
                rows = list(csv.reader(stream))[1:]
            self.db.executemany("INSERT INTO dev.dim_customer VALUES (" + ",".join("?" for _ in COLUMNS) + ")", rows)
            return Result()
        statement = statement.replace("`dev`.`gold`.", "dev.").replace("`dev`.`silver`.", "dev.")
        if statement.startswith("CREATE OR REPLACE TEMP VIEW"):
            self.db.execute("DROP VIEW IF EXISTS customer_gold_stage")
            statement = statement.replace("CREATE OR REPLACE TEMP VIEW", "CREATE TEMP VIEW")
        if statement.startswith("MERGE"):
            assert "MIN(`valid_from`)" in statement
            assert f"t.`valid_to` = '{END_OF_TIME}'" in statement
            statement = f"""UPDATE dev.dim_customer AS t SET valid_to =
                (SELECT MIN(valid_from) FROM customer_gold_stage s WHERE s.wwi_customer_id = t.wwi_customer_id)
                WHERE valid_to = '{END_OF_TIME}' AND EXISTS
                (SELECT 1 FROM customer_gold_stage s WHERE s.wwi_customer_id = t.wwi_customer_id)"""
        cursor = self.db.execute(statement)
        return Result(cursor.fetchall() if cursor.description else [])


def customer(**overrides):
    return dict(wwi_customer_id=10, customer="Same", bill_to_customer="Bill To", category="Category",
                buying_group="Group", primary_contact="Contact", postal_code="01234",
                valid_from=START_OF_TIME, valid_to=END_OF_TIME) | overrides


def insert(spark, table, columns, row):
    spark.db.execute(f"INSERT INTO dev.{table} VALUES (" + ",".join("?" for _ in columns) + ")",
                     [row[c] for c in columns])


def test_staged_versions_preserve_duplicates_noops_order_and_precision():
    spark = Spark()
    unknown = customer(customer_key=0, wwi_customer_id=0, customer="Unknown", lineage_key=0)
    old = customer(customer_key=402, lineage_key=2)
    historical = customer(customer_key=5, valid_to="2015-01-01 00:00:00.0000000", lineage_key=2)
    for row in (unknown, old, historical):
        insert(spark, "dim_customer", COLUMNS, row)
    first, later = "2016-06-07 01:00:00.0000001", "2016-06-08 01:00:00.0000002"
    # Feed later before earlier; both earlier versions and all unchanged attributes survive.
    for row in (customer(valid_from=later), customer(valid_from=first, valid_to=later),
                customer(valid_from=first, valid_to=later)):
        insert(spark, "customer_changes", SOURCE_COLUMNS, row)
    run(spark, CONTRACT, drop="D1", **ARGS)
    assert spark.db.execute("SELECT customer_key, valid_from, valid_to, lineage_key FROM dev.dim_customer ORDER BY customer_key").fetchall() == [
        (0, START_OF_TIME, END_OF_TIME, 0),
        (5, START_OF_TIME, historical["valid_to"], 2),
        (402, START_OF_TIME, first, 2),
        (403, first, later, 15), (404, first, later, 15),
        (405, later, END_OF_TIME, 15)]
    assert spark.db.execute("SELECT postal_code FROM dev.dim_customer WHERE customer_key = 403").fetchone() == ("01234",)
    spark.db.execute("DELETE FROM dev.customer_changes")
    spark.lineage = ((28,),)
    final = "2016-06-15 00:00:00.0000003"
    insert(spark, "customer_changes", SOURCE_COLUMNS, customer(valid_from=final))
    run(spark, CONTRACT, drop="D2", **(ARGS | dict(last_cutoff=ARGS["new_cutoff"], new_cutoff="2016-06-21 00:00:00.0000000")))
    assert spark.db.execute("SELECT customer_key, valid_to, lineage_key FROM dev.dim_customer WHERE customer_key >= 405 ORDER BY customer_key").fetchall() == [
        (405, final, 15), (406, END_OF_TIME, 28)]


def test_related_event_keeps_customer_validity_before_window():
    spark = Spark()
    insert(spark, "dim_customer", COLUMNS, customer(customer_key=402, lineage_key=2))
    insert(spark, "customer_changes", SOURCE_COLUMNS, customer(category="Updated category"))
    run(spark, CONTRACT, drop="D1", **ARGS)
    assert spark.db.execute("SELECT customer_key, category, valid_from FROM dev.dim_customer WHERE customer_key = 403").fetchone() == (
        403, "Updated category", START_OF_TIME)


def test_seed_and_empty_windows_match_all_golden_drops():
    spark = Spark()
    for drop in ("D0", "D1", "D2", "D0"):
        run(spark, CONTRACT, drop=drop, **ARGS)
        with (ROOT / f"golden/outputs/{drop}/tables/Dimension.Customer.csv").open() as stream:
            expected = Counter(tuple(r) for r in list(csv.reader(stream))[1:])
        actual = Counter(tuple(str(v) for v in r) for r in spark.db.execute("SELECT * FROM dev.dim_customer"))
        assert actual == expected
        assert sum(actual.values()) == 403


@pytest.mark.parametrize("lineage", [(), ((15,), (28,))])
def test_lineage_must_be_unambiguous(lineage):
    spark = Spark(lineage)
    with pytest.raises(ValueError, match="one P5 Customer lineage row"):
        run(spark, CONTRACT, drop="D1", **ARGS)
    assert len(spark.statements) == 1


@pytest.mark.parametrize("overrides", [dict(drop="D3"), dict(new_cutoff="bad"),
                                      dict(new_cutoff="2016-01-01 00:00:00.0000000")])
def test_invalid_run_parameters(overrides):
    spark = Spark()
    with pytest.raises(ValueError):
        run(spark, CONTRACT, **(ARGS | dict(drop="D1") | overrides))
    assert spark.statements == []


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
@pytest.mark.parametrize("status", ["PASS", "FAIL"])
def test_visible_parity_task_uses_drop_contract(monkeypatch, drop, status):
    def compare(warehouse, entry, path, catalog, schema):
        assert entry["table"] == "gold.dim_customer" and entry["drop"] == drop
        assert entry["path"] == f"outputs/{drop}/tables/Dimension.Customer.csv"
        return dict(status=status, reason="row mismatch")
    monkeypatch.setattr("sql.units.wwi_dim_customer_to_gold.verify.compare_entry", compare)
    kwargs = {key: ARGS[key] for key in ("catalog", "gold_schema", "golden_path")}
    if status == "PASS":
        assert verify(None, CONTRACT, drop=drop, **kwargs)["status"] == "PASS"
    else:
        with pytest.raises(RuntimeError, match="dim_customer parity failed"):
            verify(None, CONTRACT, drop=drop, **kwargs)


def test_unit_ownership_and_visible_job_tasks():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry["wwi_dim_customer_to_gold"] == dict(job="wwi_dim_customer_to_gold",
        outputs=["gold.dim_customer"], owns=[], depends_on=["p5_gold_seed", "wwi_customer_to_silver"])
    job = yaml.safe_load((ROOT / "resources/wwi_dim_customer_to_gold.job.yml").read_text())["resources"]["jobs"]["wwi_dim_customer_to_gold"]
    assert [task["task_key"] for task in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "load"}]
    assert {p["name"] for p in job["parameters"]} == {"drop", "last_cutoff", "new_cutoff"}
    assert "permissions" not in job
