import csv
import json
from collections import Counter
from pathlib import Path

import pytest
import yaml

from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_dim_date_to_gold.job import run
from sql.units.wwi_dim_date_to_gold.verify_date import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ARGS = dict(catalog="dev", gold_schema="gold_pr_0", golden_path="/golden")
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_date" and e["drop"] == "D0")


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows


class Spark:
    def __init__(self, counts=(1461, 1461, 0, 0, 0)):
        self.statements = []
        self.counts = counts

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        if "SELECT (SELECT count(*) FROM golden)" in statement:
            return Result([self.counts])
        return Result()


def test_seed_sequence_and_reruns():
    spark = Spark()
    for drop in ("D0", "D0", "D1", "D1", "D2", "D2"):
        run(spark, CONTRACT, drop=drop, **ARGS)
    assert len(set(spark.statements)) == 1
    sql = spark.statements[0]
    assert sql.startswith("CREATE OR REPLACE TABLE `dev`.`gold_pr_0`.`dim_date`")
    assert "/outputs/D0/tables/Dimension.Date.csv" in sql
    assert sql.count(" AS `") == 14
    baseline = None
    for drop in ("D0", "D1", "D2"):
        entry = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_date" and e["drop"] == drop)
        with (ROOT / "golden" / entry["path"]).open() as stream:
            reader = csv.reader(stream)
            assert next(reader) == ENTRY["columns"]
            rows = Counter(tuple(row) for row in reader)
        assert sum(rows.values()) == entry["row_count"] == 1461
        assert min(row[0] for row in rows) == "2013-01-01"
        assert max(row[0] for row in rows) == "2016-12-31"
        if baseline is None:
            baseline = rows
        assert rows == baseline


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_parity_uses_run_drop(drop):
    spark = Spark()
    assert parity(spark, CONTRACT, drop=drop, **ARGS)["status"] == "PASS"
    sql = spark.statements[-1]
    assert f"/outputs/{drop}/tables/Dimension.Date.csv" in sql
    assert "EXCEPT ALL SELECT * FROM actual" in sql
    assert "EXCEPT ALL SELECT * FROM golden" in sql


@pytest.mark.parametrize("counts", [(1461, 0, 1461, 0, 0), (1461, 1461, 1, 1, 0), (0, 0, 0, 0, 0)])
def test_parity_rejects_empty_and_changed_rows(counts):
    with pytest.raises(RuntimeError, match="dim_date parity failed"):
        parity(Spark(counts), CONTRACT, drop="D1", **ARGS)


def test_invalid_drop_and_sql_error():
    spark = Spark()
    for function in (run, parity):
        with pytest.raises(ValueError):
            function(spark, CONTRACT, drop="D3", **ARGS)
    assert spark.statements == []

    class BrokenSpark:
        def sql(self, statement):
            raise RuntimeError("storage unavailable")

    for function in (run, parity):
        with pytest.raises(RuntimeError, match="storage unavailable"):
            function(BrokenSpark(), CONTRACT, drop="D0", **ARGS)


def test_job_tasks_and_registration():
    job = yaml.safe_load((ROOT / "resources/wwi_dim_date_to_gold.job.yml").read_text())["resources"]["jobs"]["wwi_dim_date_to_gold"]
    assert job["name"] == "${var.job_name_prefix}wwi_dim_date_to_gold"
    assert {p["name"] for p in job["parameters"]} == {"drop", "last_cutoff", "new_cutoff"}
    assert job["permissions"] == [dict(group_name="users", level="CAN_VIEW")]
    assert [t["task_key"] for t in job["tasks"]] == ["seed", "parity"]
    assert job["tasks"][1]["depends_on"] == [dict(task_key="seed")]
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry["wwi_dim_date_to_gold"] == dict(job="wwi_dim_date_to_gold", outputs=["gold.dim_date"], owns=[], depends_on=["p5_gold_seed"])
