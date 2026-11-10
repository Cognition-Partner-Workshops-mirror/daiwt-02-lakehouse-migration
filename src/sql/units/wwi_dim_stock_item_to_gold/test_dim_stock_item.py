import csv
import json
from pathlib import Path

import pytest
import yaml

from classify_changes import classify
from sql.units.p5_gold_seed.integration import ENTITIES
from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_dim_stock_item_to_gold.job import CHECKPOINT, run
from sql.units.wwi_dim_stock_item_to_gold.verify_stock_item import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_stock_item" and e["drop"] == "D0")
ARGS = dict(catalog="dev", gold_schema="gold_pr_0", silver_schema="silver_pr_0", golden_path="/golden")
CUTOFFS = ["2016-06-06 12:31:17.0000000", "2016-06-14 00:00:00.0000000", "2016-06-21 00:00:00.0000000"]


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self, count=2, counts=(672, 672, 0, 0, 0)):
        self.statements, self.state = [], {}
        self.window = [("D1", CUTOFFS[0], CUTOFFS[1])]
        self.count, self.counts = count, counts
        self.fail_insert = False

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT drop"):
            return Result(self.window)
        if statement.startswith("SHOW TBLPROPERTIES"):
            return Result([(CHECKPOINT, json.dumps(self.state))] if self.state else [])
        if " SET TBLPROPERTIES " in statement:
            self.state = json.loads(statement.split(" = '", 1)[1][:-2])
        if " UNSET TBLPROPERTIES " in statement:
            self.state = {}
        if statement.startswith("SELECT lineage_key"):
            return Result([(i + 1, name) for i, name in enumerate(ENTITIES)])
        if statement.startswith("SELECT count(*)"):
            return Result([(self.count,)])
        if statement.startswith("SELECT coalesce(max"):
            return Result([(671,)])
        if statement.startswith("DESCRIBE HISTORY"):
            return Result([(7,)])
        if statement.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        if "SELECT (SELECT count(*) FROM golden)" in statement:
            return Result([self.counts])
        if self.fail_insert and statement.startswith("INSERT INTO"):
            raise RuntimeError("write failed")
        return Result()


def load(spark, drop="D1", **kwargs):
    index = int(drop[1])
    run(spark, CONTRACT, **ARGS, drop=drop,
        last_cutoff=CUTOFFS[max(0, index - 1)], new_cutoff=CUTOFFS[index], **kwargs)


def test_d0_seed_and_reset():
    spark = Spark()
    spark.state = {"D1": {"complete": True}}
    load(spark, "D0")
    sql = spark.statements[0]
    assert sql.startswith("CREATE OR REPLACE TABLE")
    assert "/outputs/D0/tables/Dimension.Stock_Item.csv" in sql
    assert "DECIMAL(18,3)" in sql and "DECIMAL(18,2)" in sql
    assert spark.state == {}
    with (ROOT / "golden" / ENTRY["path"]).open() as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 672
    assert any(row["stock_item_key"] == "0" for row in rows)


@pytest.mark.parametrize("count", [0, 2])
def test_d1_d2_sql_and_reservation(count):
    spark = Spark(count)
    for drop, index in (("D1", 1), ("D2", 2)):
        spark.window = [(drop, CUTOFFS[index - 1], CUTOFFS[index])]
        load(spark, drop)
        checkpoint = spark.state[drop]
        hwm = 671 + (count if drop == "D2" else 0)
        assert checkpoint == dict(version=7, hwm=hwm, reserved_hwm=hwm + count,
                                  count=count, window=CUTOFFS[index - 1:index + 1], complete=True)
    statements = "\n".join(spark.statements)
    assert "MIN(`valid_from`)" in statements
    assert "671 + ROW_NUMBER() OVER (ORDER BY `valid_from`, `wwi_stock_item_id`," in statements
    assert "upper(hex(`photo`))" in statements
    assert "CAST(`is_chiller_stock` AS BIGINT)" in statements
    assert "CAST(`tax_rate` AS DECIMAL(18,3))" in statements
    assert "CAST(`unit_price` AS DECIMAL(18,2))" in statements
    assert f"valid_from > '{CUTOFFS[0]}' AND valid_from <= '{CUTOFFS[1]}'" in statements
    assert "rpad(" in statements and "CAST(5 AS BIGINT) AS `lineage_key`" in statements
    assert "UPDATE `dev`.`gold_pr_0`.`integration" not in statements
    assert "DISTINCT" not in statements


@pytest.mark.parametrize("failed", [False, True])
def test_retry_restores_before_mutation(failed):
    spark = Spark()
    spark.fail_insert = failed
    if failed:
        with pytest.raises(RuntimeError, match="write failed"):
            load(spark)
        assert not spark.state["D1"]["complete"]
    else:
        load(spark)
    spark.fail_insert = False
    spark.statements = []
    load(spark)
    restore = next(i for i, s in enumerate(spark.statements) if s.startswith("RESTORE TABLE"))
    merge = next(i for i, s in enumerate(spark.statements) if s.startswith("MERGE"))
    assert "TO VERSION AS OF 7" in spark.statements[restore]
    assert restore < merge
    assert spark.state["D1"]["hwm"] == 671
    assert spark.state["D1"]["complete"]
    spark.window = [("D2", CUTOFFS[1], CUTOFFS[2])]
    load(spark, "D2")
    load(spark, "D2")
    assert spark.state["D1"]["complete"] and spark.state["D2"]["complete"]


@pytest.mark.parametrize("window", [[], [("D0", CUTOFFS[0], CUTOFFS[1])],
                                  [("D1", CUTOFFS[0], CUTOFFS[2])]])
def test_window_mismatch(window):
    spark = Spark()
    spark.window = window
    with pytest.raises(ValueError, match="run Stock Item silver for this window first"):
        load(spark)
    assert not any(s.startswith("MERGE") for s in spark.statements)


def test_d2_requires_d1_and_rejects_changed_retries():
    spark = Spark()
    spark.window = [("D2", CUTOFFS[1], CUTOFFS[2])]
    with pytest.raises(ValueError, match="D2 requires D1 complete"):
        load(spark, "D2")
    spark.window = [("D1", CUTOFFS[0], CUTOFFS[1])]
    load(spark)
    spark.count = 3
    with pytest.raises(ValueError, match="stage count mismatch"):
        load(spark)
    spark.count = 2
    spark.state["D1"]["window"][0] = CUTOFFS[1]
    with pytest.raises(ValueError, match="Checkpoint window mismatch"):
        load(spark)


@pytest.mark.parametrize("drop,last,new", [
    ("D3", CUTOFFS[0], CUTOFFS[1]),
    ("D0", "not a cutoff", CUTOFFS[1]),
    ("D1", CUTOFFS[2], CUTOFFS[1]),
])
def test_invalid_parameters(drop, last, new):
    spark = Spark()
    with pytest.raises(ValueError):
        run(spark, CONTRACT, **ARGS, drop=drop, last_cutoff=last, new_cutoff=new)
    assert spark.statements == []


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_parity(drop):
    spark = Spark()
    args = {k: v for k, v in ARGS.items() if k != "silver_schema"}
    assert parity(spark, CONTRACT, **args, drop=drop)["status"] == "PASS"
    assert f"/outputs/{drop}/tables/Dimension.Stock_Item.csv" in spark.statements[-1]
    assert "EXCEPT ALL SELECT * FROM actual" in spark.statements[-1]
    assert "EXCEPT ALL SELECT * FROM golden" in spark.statements[-1]


@pytest.mark.parametrize("counts", [(0, 0, 0, 0, 0), (672, 0, 672, 0, 0), (672, 672, 1, 1, 0)])
def test_parity_failure(counts):
    with pytest.raises(RuntimeError, match="dim_stock_item parity failed"):
        parity(Spark(counts=counts), CONTRACT, catalog="dev", gold_schema="gold_pr_0",
               golden_path="/golden", drop="D1")


def test_resource_registry_and_classifier():
    unit = "wwi_dim_stock_item_to_gold"
    resource = f"resources/{unit}.job.yml"
    job = yaml.safe_load((ROOT / resource).read_text())["resources"]["jobs"][unit]
    assert job["name"] == "${var.job_name_prefix}" + unit
    assert job["max_concurrent_runs"] == 1
    assert "permissions" not in job
    assert {p["name"] for p in job["parameters"]} == {"drop", "last_cutoff", "new_cutoff"}
    assert [t["task_key"] for t in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "load"}]
    assert all("run_job_task" not in t for t in job["tasks"])
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["silver_schema"] == "${var.silver_schema}"
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry[unit] == dict(job=unit, outputs=["gold.dim_stock_item"], owns=[],
                                 depends_on=["p5_gold_seed", "wwi_stock_item_to_silver"])
    base = {k: v for k, v in registry.items() if k != unit}
    expected = {"bronze_landing_replay", "silver_temporal_views", "p5_gold_seed",
                "wwi_stock_item_to_silver", unit, "wwi_fact_sale_to_gold",
                "wwi_fact_stock_holding_to_gold"}
    assert set(classify(registry, [resource, "src/sql/units/registry.json"], base)) == expected
    assert classify(registry, [f"src/sql/units/{unit}/job.py"]) == [unit, "wwi_fact_sale_to_gold",
                                                                         "wwi_fact_stock_holding_to_gold"]
