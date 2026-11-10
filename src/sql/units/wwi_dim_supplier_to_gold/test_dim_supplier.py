import csv
import json
import sqlite3
from collections import Counter
from pathlib import Path

import pytest
import yaml

from sql.framework import END_OF_TIME, START_OF_TIME, scd2
from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_dim_supplier_to_gold.job import COLUMNS, run
from sql.units.wwi_dim_supplier_to_gold.verify_supplier import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_supplier" and e["drop"] == "D0")
ARGS = dict(catalog="dev", gold_schema="gold_pr_0", silver_schema="silver_pr_0",
            golden_path="/golden", last_cutoff="2016-01-01 00:00:00.0000000",
            new_cutoff="2016-01-02 00:00:00.0000000")
PARITY_ARGS = {k: ARGS[k] for k in ("catalog", "gold_schema", "golden_path")}


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self, counts=(28, 28, 0, 0, 0), fail_insert=False):
        self.statements = []
        self.counts = counts
        self.fail_insert = fail_insert

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT lineage_key"):
            from sql.units.p5_gold_seed.integration import ENTITIES
            return Result([(30 + i, entity) for i, entity in enumerate(ENTITIES)])
        if statement.startswith("SELECT coalesce(max(`supplier_key`)"):
            return Result([(27,)])
        if statement.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        if "SELECT (SELECT count(*) FROM golden)" in statement:
            return Result([self.counts])
        if self.fail_insert and statement.startswith("INSERT INTO"):
            raise RuntimeError("storage unavailable")
        return Result()


def test_d0_seed_and_unchanged_golden_windows():
    spark = Spark()
    run(spark, CONTRACT, drop="D0", **ARGS)
    assert len(spark.statements) == 1
    assert "CREATE OR REPLACE TABLE `dev`.`gold_pr_0`.`dim_supplier`" in spark.statements[0]
    assert "/outputs/D0/tables/Dimension.Supplier.csv" in spark.statements[0]
    snapshots = []
    for drop in ("D0", "D1", "D2"):
        with (ROOT / f"golden/outputs/{drop}/tables/Dimension.Supplier.csv").open() as stream:
            snapshots.append(Counter(tuple(r[c] for c in ENTRY["columns"]) for r in csv.DictReader(stream)))
    assert snapshots[0] == snapshots[1] == snapshots[2]
    assert sum(snapshots[0].values()) == 28
    assert max(int(r[0]) for r in snapshots[0]) == 27
    assert any(r[0] == "0" for r in snapshots[0])


@pytest.mark.parametrize("drop", ["D1", "D2"])
def test_incremental_uses_framework_and_run_lineage(drop):
    spark = Spark()
    run(spark, CONTRACT, drop=drop, **ARGS)
    stage = next(s for s in spark.statements if s.startswith("CREATE OR REPLACE TEMP"))
    assert "35 AS lineage_key" in stage
    assert "CAST(`wwi_supplier_id` AS BIGINT) AS `wwi_supplier_id`" in stage
    assert "CAST(`payment_days` AS BIGINT) AS `payment_days`" in stage
    assert "FROM `dev`.`silver_pr_0`.`supplier_changes`" in stage
    close, insert = scd2("dev.gold_pr_0.dim_supplier", "g07_supplier_stage",
                         key="supplier_key", business_key="wwi_supplier_id",
                         columns=(*COLUMNS, "lineage_key"), staging_order=COLUMNS,
                         key_high_water_mark=27)
    assert spark.statements.index(close) < spark.statements.index(insert)
    assert "MIN(`valid_from`)" in close
    assert "27 + ROW_NUMBER()" in insert
    assert "DISTINCT" not in insert
    assert any("was_successful = 1 WHERE lineage_key = 35" in s for s in spark.statements)
    assert spark.statements[-1] == "DROP VIEW IF EXISTS g07_supplier_stage"


def test_insert_all_occurrences_and_empty_stage():
    db = sqlite3.connect(":memory:")
    columns = (*COLUMNS, "lineage_key")
    db.execute("CREATE TABLE stage (" + ", ".join(columns) + ")")
    db.execute("CREATE TABLE dim (supplier_key, " + ", ".join(columns) + ")")
    row = (3, "Supplier", "Category", "Contact", None, 30, "123", START_OF_TIME, END_OF_TIME, 35)
    db.executemany("INSERT INTO stage VALUES (" + ",".join("?" for _ in row) + ")", [row, row])
    insert = scd2("dim", "stage", key="supplier_key", business_key="wwi_supplier_id",
                  columns=columns, staging_order=COLUMNS, key_high_water_mark=27)[1]
    db.execute(insert)
    assert db.execute("SELECT supplier_key FROM dim ORDER BY supplier_key").fetchall() == [(28,), (29,)]
    assert db.execute("SELECT * FROM dim WHERE supplier_key = 28").fetchone()[1:] == row
    db.execute("DELETE FROM stage")
    db.execute(insert)
    assert db.execute("SELECT count(*) FROM dim").fetchone() == (2,)


def test_validation_and_storage_error():
    for overrides in (dict(drop="D3"), dict(drop="D1", new_cutoff="bad"),
                      dict(drop="D1", new_cutoff="2015-01-01 00:00:00.0000000")):
        spark = Spark()
        with pytest.raises(ValueError):
            run(spark, CONTRACT, **{**ARGS, **overrides})
        assert not spark.statements
    spark = Spark(fail_insert=True)
    with pytest.raises(RuntimeError, match="storage unavailable"):
        run(spark, CONTRACT, drop="D1", **ARGS)
    assert not any("UPDATE `dev`.`gold_pr_0`.`integration_lineage`" in s for s in spark.statements)
    assert spark.statements[-1] == "DROP VIEW IF EXISTS g07_supplier_stage"


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_parity(drop):
    spark = Spark()
    assert parity(spark, CONTRACT, drop=drop, **PARITY_ARGS)["status"] == "PASS"
    assert f"/outputs/{drop}/tables/Dimension.Supplier.csv" in spark.statements[-1]
    assert "EXCEPT ALL SELECT * FROM actual" in spark.statements[-1]


@pytest.mark.parametrize("counts", [(28, 0, 28, 0, 0), (28, 28, 1, 1, 0), (0, 0, 0, 0, 0)])
def test_parity_rejects_mismatch_and_empty(counts):
    with pytest.raises(RuntimeError, match="dim_supplier parity failed"):
        parity(Spark(counts), CONTRACT, drop="D1", **PARITY_ARGS)


def test_job_registration():
    job = yaml.safe_load((ROOT / "resources/wwi_dim_supplier_to_gold.job.yml").read_text())["resources"]["jobs"]["wwi_dim_supplier_to_gold"]
    assert "permissions" not in job
    assert [t["task_key"] for t in job["tasks"]] == ["apply", "parity"]
    assert job["tasks"][1]["depends_on"] == [dict(task_key="apply")]
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry["wwi_dim_supplier_to_gold"] == dict(job="wwi_dim_supplier_to_gold", outputs=["gold.dim_supplier"], owns=[], depends_on=["p5_gold_seed", "s06_supplier"])
