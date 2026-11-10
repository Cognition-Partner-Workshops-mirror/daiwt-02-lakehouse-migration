import csv
import json
from pathlib import Path

import pytest
import yaml

from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_dim_employee_to_gold.job import run
from sql.units.wwi_dim_employee_to_gold.verify import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ARGS = dict(catalog="dev", gold_schema="gold_pr_0", golden_path="/golden")
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_employee" and e["drop"] == "D0")
LOAD_ARGS = dict(**ARGS, silver_schema="silver_pr_0",
                 last_cutoff="2016-06-14 00:00:00.0000000",
                 new_cutoff="2016-06-21 00:00:00.0000000")


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self, counts=(214, 214, 0, 0, 0)):
        self.statements = []
        self.counts = counts

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        if "SELECT (SELECT count(*) FROM golden)" in statement:
            return Result([self.counts])
        return Result()


class LoadSpark(Spark):
    def __init__(self, lineage=((29,),), applied=False):
        super().__init__()
        self.lineage = lineage
        self.applied = applied

    def sql(self, statement):
        result = super().sql(statement)
        if statement.startswith("SELECT lineage_key"):
            return Result(self.lineage)
        if statement.startswith("SELECT 1"):
            return Result([(1,)] if self.applied else [])
        if statement.startswith("SELECT coalesce(max"):
            return Result([(212,)])
        return result


def golden(drop):
    entry = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_employee" and e["drop"] == drop)
    with (ROOT / "golden" / entry["path"]).open() as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == ENTRY["columns"]
        rows = {row["employee_key"]: row for row in reader}
    assert len(rows) == entry["row_count"]
    return rows


def test_d0_seed_only():
    spark = Spark()
    run(spark, CONTRACT, drop="D0", **LOAD_ARGS)
    assert len(spark.statements) == 1
    sql = spark.statements[0]
    assert sql.startswith("CREATE OR REPLACE TABLE `dev`.`gold_pr_0`.`dim_employee`")
    assert "/outputs/D0/tables/Dimension.Employee.csv" in sql


def test_golden_d2_is_one_scd2_version_after_hwm():
    d0, d1, d2 = golden("D0"), golden("D1"), golden("D2")
    assert d0 == d1
    assert d0["0"]["employee"] == "Unknown" and d0["0"]["lineage_key"] == "0"
    assert max(map(int, d1)) == 212
    added = set(d2) - set(d1)
    assert added == {"213"} and d2["213"]["lineage_key"] == "29"
    changed = {k for k in d1 if d1[k] != d2[k]}
    assert changed == {"203"}
    closed = d2["203"]
    assert closed["wwi_employee_id"] == d2["213"]["wwi_employee_id"]
    assert closed["valid_to"] == d2["213"]["valid_from"]
    assert closed["lineage_key"] == d1["203"]["lineage_key"]


@pytest.mark.parametrize("drop", ["D1", "D2"])
def test_scd2_load(drop):
    spark = LoadSpark()
    run(spark, CONTRACT, drop=drop, **LOAD_ARGS)
    lookup, guard, stage, hwm, close, insert = spark.statements
    assert "table_name = 'Employee'" in lookup
    assert "source_system_cutoff_time = '2016-06-21 00:00:00.0000000'" in lookup
    assert "lineage_key = 29 LIMIT 1" in guard
    assert stage.startswith("CREATE OR REPLACE TEMP VIEW")
    assert "`dev`.`silver_pr_0`.`employee_changes`" in stage
    for cast in ("CAST(wwi_employee_id AS BIGINT)", "CAST(employee AS STRING)",
                 "CAST(preferred_name AS STRING)", "CAST(is_salesperson AS BIGINT)",
                 "upper(CAST(photo AS STRING))", "CAST(29 AS BIGINT) AS lineage_key"):
        assert cast in stage
    assert stage.count("Invalid datetime2(7) string") == 2
    assert "max(`employee_key`)" in hwm
    assert close.startswith("MERGE INTO `dev`.`gold_pr_0`.`dim_employee`")
    assert "MIN(`valid_from`)" in close
    assert "9999-12-31 23:59:59.9999999" in close
    assert "lineage_key" not in close
    assert insert.startswith("INSERT INTO")
    assert "212 + ROW_NUMBER() OVER (ORDER BY `valid_from`, `wwi_employee_id`," in insert
    assert "`lineage_key`" in insert
    assert "WHERE" not in insert


def test_existing_lineage_skips():
    spark = LoadSpark(applied=True)
    run(spark, CONTRACT, drop="D2", **LOAD_ARGS)
    assert len(spark.statements) == 2


@pytest.mark.parametrize("lineage", [(), ((1,), (2,))])
def test_missing_or_duplicate_lineage_raises(lineage):
    spark = LoadSpark(lineage=lineage)
    with pytest.raises(ValueError, match="exactly one"):
        run(spark, CONTRACT, drop="D1", **LOAD_ARGS)
    assert len(spark.statements) == 1


@pytest.mark.parametrize("overrides", [
    dict(drop="D3"), dict(last_cutoff="bad"), dict(new_cutoff="2016-02-30 00:00:00.0000000"),
    dict(last_cutoff="2016-06-22 00:00:00.0000000"),
])
def test_invalid_load_parameters(overrides):
    spark = Spark()
    with pytest.raises(ValueError):
        run(spark, CONTRACT, **{**LOAD_ARGS, "drop": "D1", **overrides})
    assert spark.statements == []


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_parity_uses_run_drop(drop):
    rows = 214 if drop == "D2" else 213
    spark = Spark((rows, rows, 0, 0, 0))
    assert parity(spark, CONTRACT, drop=drop, **ARGS)["status"] == "PASS"
    sql = spark.statements[-1]
    assert f"/outputs/{drop}/tables/Dimension.Employee.csv" in sql
    assert "EXCEPT ALL SELECT * FROM actual" in sql
    assert "EXCEPT ALL SELECT * FROM golden" in sql


@pytest.mark.parametrize("counts", [(214, 0, 214, 0, 0), (214, 214, 1, 1, 0), (0, 0, 0, 0, 0)])
def test_parity_rejects_empty_and_changed_rows(counts):
    with pytest.raises(RuntimeError, match="dim_employee parity failed"):
        parity(Spark(counts), CONTRACT, drop="D2", **ARGS)


def test_sql_error_propagates():
    class BrokenSpark:
        def sql(self, statement):
            raise RuntimeError("storage unavailable")

    for function, args in ((run, LOAD_ARGS), (parity, ARGS)):
        with pytest.raises(RuntimeError, match="storage unavailable"):
            function(BrokenSpark(), CONTRACT, drop="D0", **args)


def test_job_tasks_and_registration():
    job = yaml.safe_load((ROOT / "resources/wwi_dim_employee_to_gold.job.yml").read_text())["resources"]["jobs"]["wwi_dim_employee_to_gold"]
    assert job["name"] == "${var.job_name_prefix}wwi_dim_employee_to_gold"
    assert {p["name"] for p in job["parameters"]} == {"drop", "last_cutoff", "new_cutoff"}
    assert "permissions" not in job
    assert [t["task_key"] for t in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][1]["depends_on"] == [dict(task_key="load")]
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry["wwi_dim_employee_to_gold"] == dict(
        job="wwi_dim_employee_to_gold", outputs=["gold.dim_employee"], owns=[],
        depends_on=["p5_gold_seed", "wwi_employee_to_silver"])
    for task, notebook in zip(job["tasks"], ("job.py", "verify.py")):
        assert task["notebook_task"]["notebook_path"].endswith("/wwi_dim_employee_to_gold/" + notebook)
        assert task["notebook_task"]["base_parameters"] == {
            name: "${var." + name + "}" for name in
            ("catalog", "gold_schema", "silver_schema", "golden_path")}
