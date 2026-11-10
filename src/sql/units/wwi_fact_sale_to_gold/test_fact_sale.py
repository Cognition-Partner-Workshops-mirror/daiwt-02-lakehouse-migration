import json
from pathlib import Path

import pytest
import yaml

from classify_changes import classify
from sql.units.p5_gold_seed.integration import ENTITIES
from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_fact_sale_to_gold.job import LOOKUPS, insert_select, run, stage_select
from sql.units.wwi_fact_sale_to_gold.verify_sale import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.fact_sale" and e["drop"] == "D0")
ARGS = dict(catalog="dev", gold_schema="gold_pr_0", silver_schema="silver_pr_0",
            bronze_schema="bronze_pr_0", golden_path="/golden",
            last_cutoff="2016-06-06 12:31:17.0000000", new_cutoff="2016-06-14 00:00:00.0000000")


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self, latest="D1", unmatched=0, fail=False, counts=None):
        self.statements, self.latest, self.unmatched, self.fail = [], latest, unmatched, fail
        self.counts = counts or (ENTRY["row_count"],) * 2 + (0, 0, 0)

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT DISTINCT"):
            return Result([(self.latest,)] if self.latest else [])
        if "WHERE _line_id IS NULL" in statement:
            return Result([(self.unmatched,)])
        if statement.startswith("SELECT lineage_key"):
            return Result([(i + 14, name) for i, name in enumerate(ENTITIES)])
        if "coalesce(max(" in statement:
            return Result([(228265,)])
        if statement.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        if "SELECT (SELECT count(*) FROM golden)" in statement:
            return Result([self.counts])
        if self.fail and statement.startswith("INSERT INTO `dev`"):
            raise RuntimeError("write failed")
        return Result()


def test_d0_seed():
    spark = Spark(latest=None)
    run(spark, CONTRACT, **ARGS, drop="D0")
    assert len(spark.statements) == 1
    assert "/outputs/D0/tables/Fact.Sale.csv" in spark.statements[0]
    assert "CREATE OR REPLACE TABLE" in spark.statements[0]


@pytest.mark.parametrize("drop,lineage", [("D1", 24), ("D2", 24)])
def test_delete_then_insert(drop, lineage):
    spark = Spark(latest=drop)
    run(spark, CONTRACT, **(ARGS | dict(drop=drop)))
    sql = spark.statements
    delete = next(i for i, s in enumerate(sql) if s.startswith("DELETE"))
    insert = next(i for i, s in enumerate(sql) if s.startswith("INSERT INTO `dev`"))
    hwm = next(i for i, s in enumerate(sql) if "coalesce(max(`sale_key`)" in s)
    assert hwm < delete < insert
    assert "WHERE wwi_invoice_id IN (SELECT wwi_invoice_id FROM sale_gold_stage_5)" in sql[delete]
    assert "228265 + ROW_NUMBER() OVER (ORDER BY wwi_invoice_id, _line_id)" in sql[insert]
    assert f"CAST({lineage} AS BIGINT) AS `lineage_key`" in sql[insert]
    assert "CAST(CAST(`unit_price` AS STRING) AS DECIMAL(18,2))" in sql[insert]
    assert "`silver_pr_0`.`sale_changes`" in sql[1]
    assert "`bronze_pr_0`.`sales_invoice_lines`" in sql[1]
    assert sql[-1].startswith("UPDATE") and f"WHERE lineage_key = {lineage}" in sql[-1]


def test_lookups_use_legacy_window():
    spark = Spark()
    run(spark, CONTRACT, **ARGS, drop="D1")
    views = [s for s in spark.statements if s.startswith("CREATE OR REPLACE TEMP VIEW sale_gold_stage_")]
    assert len(views) == 1 + len(LOOKUPS)
    for (dimension, key, business_key, natural_id, output), view in zip(LOOKUPS, views[1:]):
        assert f"`gold_pr_0`.`{dimension}`" in view
        assert f"d.`{business_key}` = f.`{natural_id}`" in view
        assert "f.`_ts` > d.`valid_from` AND f.`_ts` <= d.`valid_to`" in view
        assert f"coalesce(d.`{key}`, 0) AS `{output}`" in view
        assert f"ORDER BY d.`valid_from`, d.`{key}`" in view


def test_stage_and_insert_shape():
    stage = stage_select("silver", "lines")
    assert "ORDER BY InvoiceLineID" in stage
    assert "l.InvoiceLineID AS _line_id" in stage
    assert "s.`WWI Invoice ID` <=> l.`InvoiceID`" in stage
    insert = insert_select(ENTRY, "staged", 0, 24)
    assert insert.count(" AS `") == len(ENTRY["columns"])
    assert "CAST(`delivery_date_key` AS DATE)" in insert


@pytest.mark.parametrize("spark,error", [
    (Spark(latest="D0"), "for this drop"), (Spark(latest=None), "for this drop"),
    (Spark(unmatched=3), "without a source invoice line")])
def test_guards(spark, error):
    with pytest.raises(ValueError, match=error):
        run(spark, CONTRACT, **ARGS, drop="D1")
    assert not any(s.startswith(("DELETE", "INSERT", "UPDATE")) for s in spark.statements)


def test_failed_insert_keeps_lineage_incomplete():
    spark = Spark(fail=True)
    with pytest.raises(RuntimeError, match="write failed"):
        run(spark, CONTRACT, **ARGS, drop="D1")
    assert not any(s.startswith("UPDATE") for s in spark.statements)


@pytest.mark.parametrize("override", [dict(drop="D3"), dict(last_cutoff="bad"),
                                     dict(last_cutoff="2017-01-01 00:00:00.0000000")])
def test_invalid_window(override):
    spark = Spark()
    with pytest.raises(ValueError):
        run(spark, CONTRACT, **(ARGS | dict(drop="D1") | override))
    assert spark.statements == []


def test_parity():
    assert parity(Spark(), CONTRACT, catalog="dev", gold_schema="gold_pr_0",
                  golden_path="/golden", drop="D0")["status"] == "PASS"
    spark = Spark(counts=(ENTRY["row_count"], ENTRY["row_count"], 1, 1, 0))
    with pytest.raises(RuntimeError, match="parity failed"):
        parity(spark, CONTRACT, catalog="dev", gold_schema="gold_pr_0", golden_path="/golden", drop="D0")


def test_job_and_registry():
    unit = "wwi_fact_sale_to_gold"
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry[unit] == dict(job=unit, outputs=["gold.fact_sale"], owns=[], depends_on=[
        "p5_gold_seed", "wwi_sale_to_silver", "wwi_dim_city_to_gold", "wwi_dim_customer_to_gold",
        "wwi_dim_employee_to_gold", "wwi_dim_stock_item_to_gold"])
    assert unit in classify(registry, [f"resources/{unit}.job.yml"])
    job = yaml.safe_load((ROOT / f"resources/{unit}.job.yml").read_text())["resources"]["jobs"][unit]
    assert "permissions" not in job
    assert [t["task_key"] for t in job["tasks"]] == ["load", "parity"]
    assert job["tasks"][0]["notebook_task"]["base_parameters"]["bronze_schema"] == "${var.bronze_schema}"
