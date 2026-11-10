import json
from pathlib import Path

import pytest
import yaml

from classify_changes import classify
from sql.units.p5_gold_seed.integration import ENTITIES
from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_fact_order_to_gold.job import order_select, run
from sql.units.wwi_fact_order_to_gold.verify import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.fact_order" and e["drop"] == "D0")
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
    def __init__(self, present=True, fail=False, hwm=231412):
        self.statements = []
        self.present, self.fail, self.hwm = present, fail, hwm

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT *"):
            return Result([("D1",)] if self.present else [])
        if statement.startswith("SELECT lineage_key"):
            return Result([(i + 14, name) for i, name in enumerate(ENTITIES)])
        if statement.startswith("SELECT coalesce(max("):
            return Result([(self.hwm,)])
        if self.fail and statement.startswith("INSERT INTO"):
            raise RuntimeError("write failed")
        return Result()


def test_d0_seeds_only_order():
    spark = Spark()
    run(spark, CONTRACT, drop="D0", **ARGS)
    assert len(spark.statements) == 1
    assert "CREATE OR REPLACE TABLE `dev`.`gold`.`fact_order`" in spark.statements[0]
    assert "Fact.Order.csv" in spark.statements[0]


@pytest.mark.parametrize("drop,hwm", [("D1", 231412), ("D2", 232864), ("D1", 0)])
def test_delete_insert_and_predelete_hwm(drop, hwm):
    spark = Spark(hwm=hwm)
    run(spark, CONTRACT, drop=drop, **ARGS)
    statements = spark.statements
    hwm_index = next(i for i, s in enumerate(statements) if "max(`order_key`)" in s)
    delete_index = next(i for i, s in enumerate(statements) if s.startswith("DELETE"))
    insert_index = next(i for i, s in enumerate(statements) if s.startswith("INSERT"))
    assert hwm_index < delete_index < insert_index
    assert f"{hwm} + ROW_NUMBER() OVER (ORDER BY _stage_seq)" in statements[insert_index]
    assert f"WHERE _drop = '{drop}'" in statements[delete_index]
    assert "wwi_order_id IN" in statements[delete_index]
    assert "was_successful = 1" in statements[-1]
    assert "WHERE lineage_key = 22" in statements[-1]


def test_matching_empty_window_is_valid():
    spark = Spark()
    run(spark, CONTRACT, drop="D1", **ARGS)
    assert not any("SELECT DISTINCT" in s for s in spark.statements)
    assert "last_cutoff =" in spark.statements[0] and "new_cutoff =" in spark.statements[0]


def test_missing_window_stops_before_writes():
    spark = Spark(present=False)
    with pytest.raises(ValueError, match="run Order silver"):
        run(spark, CONTRACT, drop="D1", **ARGS)
    assert len(spark.statements) == 1


def test_insert_error_does_not_complete_lineage():
    spark = Spark(fail=True)
    with pytest.raises(RuntimeError, match="write failed"):
        run(spark, CONTRACT, drop="D1", **ARGS)
    assert not any("was_successful = 1" in s for s in spark.statements)


@pytest.mark.parametrize("overrides", [
    {"drop": "D3"}, {"last_cutoff": "invalid"},
    {"last_cutoff": "2017-01-01 00:00:00.0000000"}])
def test_invalid_inputs(overrides):
    spark = Spark()
    with pytest.raises(ValueError):
        run(spark, CONTRACT, **{**ARGS, "drop": "D1", **overrides})
    assert not spark.statements


def test_projection_and_lookup_semantics():
    sql = order_select(ENTRY, "`dev`.`silver`.`order_changes`", "dev", "gold", "D2", 35, 232864)
    assert len(ENTRY["columns"]) == 19
    for column in ENTRY["columns"]:
        assert f"AS {column_type(column)}) AS `{column}`" in sql
    assert sql.count("f.`_last_modified_when_text` > d.`valid_from`") == 5
    assert sql.count("f.`_last_modified_when_text` <= d.`valid_to`") == 5
    assert sql.count("coalesce(d.") == 5
    assert sql.count("PARTITION BY f.`_stage_seq` ORDER BY d.`valid_from`, d.") == 5
    assert "d.`wwi_employee_id` = f.`wwi_salesperson_id`" in sql
    assert "d.`wwi_employee_id` = f.`wwi_picker_id`" in sql
    assert "CAST(CAST(`unit_price` AS STRING) AS DECIMAL(18,2))" in sql
    assert "CAST(`picked_date_key` AS DATE)" in sql
    assert "CAST(`wwi_backorder_id` AS BIGINT)" in sql
    assert "CAST(35 AS BIGINT) AS `lineage_key`" in sql


@pytest.mark.parametrize("status", ["PASS", "FAIL"])
def test_parity_task(monkeypatch, status):
    result = dict(status=status, reason="test")
    def compare(warehouse, entry, path, catalog, schema):
        assert entry["table"] == "gold.fact_order" and entry["drop"] == "D2"
        return result
    monkeypatch.setattr("sql.units.wwi_fact_order_to_gold.verify.compare_entry", compare)
    args = {k: ARGS[k] for k in ("catalog", "gold_schema", "golden_path")}
    if status == "PASS":
        assert parity(Spark(), CONTRACT, drop="D2", **args) == result
    else:
        with pytest.raises(RuntimeError, match="fact_order parity failed"):
            parity(Spark(), CONTRACT, drop="D2", **args)


def test_resource_registry_classifier():
    unit = "wwi_fact_order_to_gold"
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry[unit] == dict(job=unit, outputs=["gold.fact_order"], owns=[], depends_on=[
        "p5_gold_seed", "wwi_order_to_silver", "wwi_dim_city_to_gold", "wwi_dim_customer_to_gold",
        "wwi_dim_employee_to_gold", "wwi_dim_stock_item_to_gold"])
    job = yaml.safe_load((ROOT / f"resources/{unit}.job.yml").read_text())["resources"]["jobs"][unit]
    assert "permissions" not in job and job["max_concurrent_runs"] == 1
    assert [t["task_key"] for t in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][1]["depends_on"] == [{"task_key": "load"}]
    for task, filename in zip(job["tasks"], ["job.py", "verify.py"]):
        assert task["notebook_task"]["notebook_path"] == f"../src/sql/units/{unit}/{filename}"
    for path in [f"src/sql/units/{unit}/job.py", f"resources/{unit}.job.yml"]:
        assert classify(registry, [path]) == [unit]
    for upstream in registry[unit]["depends_on"]:
        assert unit in classify(registry, [f"src/sql/units/{upstream}/job.py"])
