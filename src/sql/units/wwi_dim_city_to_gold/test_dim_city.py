import csv
import json
import sqlite3
from pathlib import Path

import pytest
import yaml

from sql.framework import END_OF_TIME, START_OF_TIME
from sql.units.wwi_dim_city_to_gold.job import run
from sql.units.wwi_dim_city_to_gold.verify_city import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_city" and e["drop"] == "D0")
ARGS = dict(catalog="dev", gold_schema="gold", silver_schema="silver", golden_path="/golden",
            last_cutoff="2016-06-06 12:31:17.0000000", new_cutoff="2016-06-07 00:00:00.0000000")


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self, lineage=((14,),)):
        self.statements = []
        self.lineage = lineage
        self.db = sqlite3.connect(":memory:")
        self.db.execute("ATTACH DATABASE ':memory:' AS dev")
        self.db.execute("CREATE TABLE dev.dim_city (" + ", ".join(ENTRY["columns"]) + ")")
        self.db.execute("CREATE TABLE dev.city_changes (" + ", ".join(
            c for c in ENTRY["columns"] if c not in ("city_key", "lineage_key")) + ", _drop, _staged_order)")

    def sql(self, sql):
        self.statements.append(sql)
        if "SELECT lineage_key FROM" in sql:
            return Result(self.lineage)
        if sql.startswith("CREATE OR REPLACE TABLE"):
            self.db.execute("DELETE FROM dev.dim_city")
            with (ROOT / "golden" / ENTRY["path"]).open() as stream:
                rows = list(csv.reader(stream))[1:]
            self.db.executemany("INSERT INTO dev.dim_city VALUES (" + ",".join("?" for _ in ENTRY["columns"]) + ")", rows)
            return Result()
        sql = sql.replace("`dev`.`gold`.", "dev.").replace("`dev`.`silver`.", "dev.")
        if sql.startswith("CREATE OR REPLACE TEMP VIEW"):
            self.db.execute("DROP VIEW IF EXISTS city_gold_stage")
            sql = sql.replace("CREATE OR REPLACE TEMP VIEW", "CREATE TEMP VIEW")
        if sql.startswith("MERGE"):
            assert "MIN(`valid_from`)" in sql
            assert f"t.`valid_to` = '{END_OF_TIME}'" in sql
            sql = f"""UPDATE dev.dim_city AS t SET valid_to =
                (SELECT MIN(valid_from) FROM city_gold_stage s WHERE s.wwi_city_id = t.wwi_city_id)
                WHERE valid_to = '{END_OF_TIME}' AND EXISTS
                (SELECT 1 FROM city_gold_stage s WHERE s.wwi_city_id = t.wwi_city_id)"""
        cursor = self.db.execute(sql)
        return Result(cursor.fetchall() if cursor.description else [])


def stage(spark, drop, order, city_id, start, end, name="A"):
    values = dict(wwi_city_id=city_id, city=name, state_province="S", country="C",
                  continent="N", sales_territory="T", region="R", subregion="SR",
                  location=None, latest_recorded_population=5, valid_from=start, valid_to=end)
    row = [values[c] for c in ENTRY["columns"] if c not in ("city_key", "lineage_key")]
    spark.db.execute("INSERT INTO dev.city_changes VALUES (" + ",".join("?" for _ in row + [drop, order]) + ")",
                     row + [drop, order])


def test_synthetic_versions_order_duplicates_and_unknown():
    spark = Spark()
    columns = ENTRY["columns"]
    baseline = dict.fromkeys(columns)
    baseline.update(city_key=0, wwi_city_id=0, city="Unknown", valid_from=START_OF_TIME, valid_to=END_OF_TIME)
    old = dict(baseline, city_key=7, wwi_city_id=10, city="Old")
    for row in (baseline, old):
        spark.db.execute("INSERT INTO dev.dim_city VALUES (" + ",".join("?" for _ in columns) + ")",
                         [row[c] for c in columns])
    first, later = "2016-06-06 13:00:00.0000001", "2016-06-06 14:00:00.0000002"
    stage(spark, "D1", 3, 10, later, END_OF_TIME, "Old")
    stage(spark, "D1", 1, 10, first, later)
    stage(spark, "D1", 2, 10, first, later)
    stage(spark, "D2", 1, 99, later, END_OF_TIME)
    run(spark, CONTRACT, drop="D1", **ARGS)
    assert spark.db.execute("SELECT city_key, city, valid_from, valid_to, lineage_key FROM dev.dim_city ORDER BY city_key").fetchall() == [
        (0, "Unknown", START_OF_TIME, END_OF_TIME, None),
        (7, "Old", START_OF_TIME, first, None),
        (8, "A", first, later, 14), (9, "A", first, later, 14),
        (10, "Old", later, END_OF_TIME, 14)]


def test_golden_empty_incremental_replay_and_reset():
    spark = Spark()
    baseline = None
    for drop in ("D0", "D1", "D2", "D0", "D1", "D2"):
        run(spark, CONTRACT, drop=drop, **ARGS)
        rows = spark.db.execute("SELECT * FROM dev.dim_city ORDER BY city_key").fetchall()
        if baseline is None:
            baseline = rows
        assert rows == baseline
    data = [(ROOT / f"golden/outputs/{d}/tables/Dimension.City.csv").read_bytes() for d in ("D0", "D1", "D2")]
    assert data[0] == data[1] == data[2]
    for drop in ("D1", "D2"):
        for table in ("Cities", "Cities_Archive", "Countries", "Countries_Archive",
                      "StateProvinces", "StateProvinces_Archive"):
            with (ROOT / f"golden/inputs/{drop}/WideWorldImporters.Application.{table}.csv").open() as stream:
                assert list(csv.DictReader(stream)) == []


@pytest.mark.parametrize("lineage", [(), ((14,), (15,))])
def test_missing_or_ambiguous_lineage(lineage):
    spark = Spark(lineage)
    with pytest.raises(ValueError, match="one P5 City lineage"):
        run(spark, CONTRACT, drop="D1", **ARGS)
    assert len(spark.statements) == 1


def test_invalid_parameters_and_storage_error():
    spark = Spark()
    for args in (dict(drop="D3"), dict(drop="D1", new_cutoff="bad"),
                 dict(drop="D1", new_cutoff="2016-01-01 00:00:00.0000000")):
        with pytest.raises(ValueError):
            run(spark, CONTRACT, **(ARGS | args))
    assert not spark.statements

    class Broken:
        def sql(self, sql):
            raise RuntimeError("storage unavailable")

    for drop in ("D0", "D1"):
        with pytest.raises(RuntimeError, match="storage unavailable"):
            run(Broken(), CONTRACT, drop=drop, **ARGS)


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
@pytest.mark.parametrize("status", ["PASS", "FAIL"])
def test_parity_uses_shared_harness(monkeypatch, drop, status):
    def compare(warehouse, entry, path, catalog, schema):
        assert entry["table"] == "gold.dim_city" and entry["drop"] == drop
        assert entry["path"] == f"outputs/{drop}/tables/Dimension.City.csv"
        return dict(status=status, reason="changed rows")
    monkeypatch.setattr("sql.units.wwi_dim_city_to_gold.verify_city.compare_entry", compare)
    kwargs = {k: ARGS[k] for k in ("catalog", "gold_schema", "golden_path")}
    if status == "FAIL":
        with pytest.raises(RuntimeError, match="dim_city parity failed"):
            parity(None, CONTRACT, drop=drop, **kwargs)
    else:
        assert parity(None, CONTRACT, drop=drop, **kwargs)["status"] == "PASS"


def test_resource_and_registry():
    job = yaml.safe_load((ROOT / "resources/wwi_dim_city_to_gold.job.yml").read_text())["resources"]["jobs"]["wwi_dim_city_to_gold"]
    assert [t["task_key"] for t in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "load"}]
    assert {p["name"] for p in job["parameters"]} == {"drop", "last_cutoff", "new_cutoff"}
    assert "permissions" not in job
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry["wwi_dim_city_to_gold"] == dict(job="wwi_dim_city_to_gold",
        outputs=["gold.dim_city"], owns=[], depends_on=["p5_gold_seed", "wwi_city_to_silver"])
