import json
import sqlite3
from collections import Counter
from pathlib import Path

import pytest
import yaml

from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_fact_purchase_to_gold.job import COLUMNS, run
from sql.units.wwi_fact_purchase_to_gold.verify_purchase import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.fact_purchase" and e["drop"] == "D0")
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
    def __init__(self, counts=(8415, 8415, 0, 0, 0), fail_insert=False):
        self.statements = []
        self.counts = counts
        self.fail_insert = fail_insert

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT lineage_key"):
            from sql.units.p5_gold_seed.integration import ENTITIES
            return Result([(30 + i, entity) for i, entity in enumerate(ENTITIES)])
        if statement.startswith("SELECT coalesce(max(`purchase_key`)"):
            return Result([(8367,)])
        if statement.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        if "SELECT (SELECT count(*) FROM golden)" in statement:
            return Result([self.counts])
        if self.fail_insert and statement.startswith("INSERT INTO"):
            raise RuntimeError("storage unavailable")
        return Result()


def test_d0_seed_only():
    spark = Spark()
    run(spark, CONTRACT, drop="D0", **ARGS)
    assert len(spark.statements) == 1
    assert "CREATE OR REPLACE TABLE `dev`.`gold_pr_0`.`fact_purchase`" in spark.statements[0]
    assert "/outputs/D0/tables/Fact.Purchase.csv" in spark.statements[0]


@pytest.mark.parametrize("drop", ["D1", "D2"])
def test_incremental_order_lookup_and_lineage(drop):
    spark = Spark()
    run(spark, CONTRACT, drop=drop, **ARGS)
    stage, supplier, stock = [s for s in spark.statements if s.startswith("CREATE OR REPLACE TEMP")]
    assert f"WHERE _staging_drop = '{drop}'" in stage
    assert "FROM `dev`.`silver_pr_0`.`purchase_staging`" in stage
    assert "rpad" in stage and "last_modified_when" in stage
    for statement, dim, key, natural_id in (
            (supplier, "dim_supplier", "supplier_key", "wwi_supplier_id"),
            (stock, "dim_stock_item", "stock_item_key", "wwi_stock_item_id")):
        assert f"LEFT JOIN `dev`.`gold_pr_0`.`{dim}`" in statement
        assert f"coalesce(d.`{key}`, 0)" in statement
        assert f"d.`{natural_id}` = f.`{natural_id}`" in statement
        assert "f.`last_modified_when` > d.`valid_from`" in statement
        assert "f.`last_modified_when` <= d.`valid_to`" in statement
        assert "PARTITION BY f.`_drop`, f.`_staged_order`" in statement
    hwm = next(s for s in spark.statements if s.startswith("SELECT coalesce(max"))
    delete = next(s for s in spark.statements if s.startswith("DELETE"))
    insert = next(s for s in spark.statements if s.startswith("INSERT"))
    assert spark.statements.index(hwm) < spark.statements.index(delete) < spark.statements.index(insert)
    assert "WHERE wwi_purchase_order_id IN (SELECT wwi_purchase_order_id FROM g11_purchase_stage)" in delete
    assert "8367 + ROW_NUMBER() OVER (ORDER BY _drop, _staged_order)" in insert
    assert "CAST(39 AS BIGINT) AS `lineage_key`" in insert
    assert ", ".join(f"`{c}`" for c in COLUMNS) in insert
    for column in COLUMNS:
        assert f"AS {column_type(column)}) AS `{column}`" in insert
    assert "DISTINCT" not in insert
    assert any("was_successful = 1 WHERE lineage_key = 39" in s for s in spark.statements)
    assert spark.statements[-3:] == [
        f"DROP VIEW IF EXISTS {v}" for v in
        ("g11_purchase_stock_item", "g11_purchase_supplier", "g11_purchase_stage")]


def test_reference_golden_drops():
    from sql.units.purchase_core import extract, manifest, read_csv, replay

    root = ROOT / "golden"
    spec = manifest(root)
    states = replay(root, spec)
    actual = read_csv(root, "outputs/D0/tables/Fact.Purchase.csv")
    accumulated = []
    for drop, lineage, first_key, count in (("D1", 23, 8368, 57), ("D2", 36, 8425, 113)):
        changes, orphans = extract(states, spec, drop)
        assert not orphans
        accumulated.extend(changes)
        hwm = max(int(r["purchase_key"]) for r in actual)
        ids = {r["wwi_purchase_order_id"] for r in accumulated}
        actual = [r for r in actual if r["wwi_purchase_order_id"] not in ids]
        dims = {dim: read_csv(root, f"outputs/{drop}/tables/Dimension.{name}.csv")
                for dim, name in (("supplier", "Supplier"), ("stock_item", "Stock_Item"))}
        for offset, row in enumerate(sorted(accumulated, key=lambda r: (r["_drop"], r["_staged_order"])), 1):
            output = {c: row[c] for c in COLUMNS if c in row}
            for dim, versions in dims.items():
                natural_id = f"wwi_{dim}_id"
                matches = sorted((d for d in versions if d[natural_id] == row[natural_id]
                                  and row["last_modified_when"] > d["valid_from"]
                                  and row["last_modified_when"] <= d["valid_to"]),
                                 key=lambda d: (d["valid_from"], int(d[f"{dim}_key"])))
                output[f"{dim}_key"] = matches[0][f"{dim}_key"] if matches else "0"
            output.update(purchase_key=str(hwm + offset), lineage_key=str(lineage))
            actual.append(output)
        expected = read_csv(root, f"outputs/{drop}/tables/Fact.Purchase.csv")
        assert Counter(tuple(r[c] for c in COLUMNS) for r in actual) == Counter(
            tuple(r[c] for c in COLUMNS) for r in expected)
        allocated = sorted(int(r["purchase_key"]) for r in actual if r["lineage_key"] == str(lineage))
        assert allocated == list(range(first_key, first_key + count))
        assert sum(r["lineage_key"] == "10" for r in actual) == 8358
    assert any(r["purchase_key"] == "8482" and r["wwi_purchase_order_id"] == "2085"
               for r in actual)


def test_empty_stage_preserves_target():
    spark = Spark()
    run(spark, CONTRACT, drop="D1", **ARGS)
    db = sqlite3.connect(":memory:")
    db.execute("ATTACH DATABASE ':memory:' AS gold_pr_0")
    columns = ", ".join(COLUMNS)
    db.execute(f"CREATE TABLE gold_pr_0.fact_purchase ({columns})")
    db.execute("INSERT INTO gold_pr_0.fact_purchase (purchase_key) VALUES (8367)")
    db.execute("CREATE TABLE g11_purchase_stage (wwi_purchase_order_id)")
    db.execute(f"CREATE TABLE g11_purchase_stock_item ({columns}, _drop, _staged_order)")
    for statement in spark.statements:
        if statement.startswith(("DELETE FROM", "INSERT INTO")):
            db.execute(statement.replace("`dev`.", ""))
    assert db.execute("SELECT purchase_key FROM gold_pr_0.fact_purchase").fetchall() == [(8367,)]
    assert any("was_successful = 1 WHERE lineage_key = 39" in s for s in spark.statements)


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
    assert spark.statements[-1] == "DROP VIEW IF EXISTS g11_purchase_stage"


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_parity(drop):
    entry = next(e for e in CONTRACT["entries"] if e["table"] == "gold.fact_purchase" and e["drop"] == drop)
    spark = Spark(counts=(entry["row_count"], entry["row_count"], 0, 0, 0))
    assert parity(spark, CONTRACT, drop=drop, **PARITY_ARGS)["status"] == "PASS"
    assert f"/outputs/{drop}/tables/Fact.Purchase.csv" in spark.statements[-1]
    assert "EXCEPT ALL SELECT * FROM actual" in spark.statements[-1]


@pytest.mark.parametrize("counts", [(28, 0, 28, 0, 0), (28, 28, 1, 1, 0), (0, 0, 0, 0, 0)])
def test_parity_rejects_mismatch_and_empty(counts):
    with pytest.raises(RuntimeError, match="fact_purchase parity failed"):
        parity(Spark(counts), CONTRACT, drop="D1", **PARITY_ARGS)


def test_job_registration():
    job = yaml.safe_load((ROOT / "resources/wwi_fact_purchase_to_gold.job.yml").read_text())["resources"]["jobs"]["wwi_fact_purchase_to_gold"]
    assert "permissions" not in job
    assert [t["task_key"] for t in job["tasks"]] == ["apply", "parity"]
    assert job["tasks"][1]["depends_on"] == [dict(task_key="apply")]
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry["wwi_fact_purchase_to_gold"] == dict(job="wwi_fact_purchase_to_gold", outputs=["gold.fact_purchase"], owns=[], depends_on=["p5_gold_seed", "wwi_purchase_to_silver", "wwi_dim_stock_item_to_gold", "wwi_dim_supplier_to_gold"])
