import csv
import json
import re
import sqlite3
from pathlib import Path

import pytest
import yaml

from sql.framework import END_OF_TIME
from sql.units.p5_gold_seed.seed import column_type
from sql.units.wwi_dim_transaction_type_to_gold import job
from sql.units.wwi_dim_transaction_type_to_gold.verify_transaction_type import run as parity

ROOT = Path(__file__).resolve().parents[4]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())
ENTRY = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_transaction_type" and e["drop"] == "D0")
ARGS = dict(catalog="dev", gold_schema="gold_pr_0", silver_schema="silver_pr_0", golden_path="/golden")
LAST = "2016-06-06 12:31:17.0000000"
NEW = "2016-06-07 12:00:00.0000000"
NEXT = "2016-06-08 12:00:00.0000000"


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class Spark:
    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE dim (transaction_type_key INT, wwi_transaction_type_id INT, transaction_type TEXT, valid_from TEXT, valid_to TEXT, lineage_key INT)")
        self.properties, self.snapshots, self.statements = {}, {}, []
        self.stage = []
        self.window = ("D1", LAST, NEW)
        self.fail_insert = False

    def rows(self):
        return self.db.execute("SELECT * FROM dim ORDER BY transaction_type_key").fetchall()

    def sql(self, statement):
        self.statements.append(statement)
        sql = statement.replace("`dev`.`gold_pr_0`.`dim_transaction_type`", "dim")
        if sql.startswith("CREATE OR REPLACE TABLE"):
            self.db.execute("DELETE FROM dim")
            with (ROOT / "golden" / ENTRY["path"]).open() as stream:
                reader = csv.reader(stream)
                next(reader)
                self.db.executemany("INSERT INTO dim VALUES (?,?,?,?,?,?)", reader)
        elif sql.startswith("ALTER TABLE dim UNSET"):
            self.properties.clear()
        elif sql.startswith("SELECT drop"):
            return Result([self.window])
        elif sql.startswith("SHOW TBLPROPERTIES"):
            return Result(list(self.properties.items()))
        elif sql.startswith("CREATE OR REPLACE TEMP VIEW"):
            self.db.execute("DROP TABLE IF EXISTS g08_transaction_type_stage")
            self.db.execute("CREATE TABLE g08_transaction_type_stage (wwi_transaction_type_id INT, transaction_type TEXT, valid_from TEXT, valid_to TEXT, lineage_key INT)")
            self.db.executemany("INSERT INTO g08_transaction_type_stage VALUES (?,?,?,?,?)",
                                [(*r, 20 if self.window[0] == "D1" else 33) for r in self.stage if self.window[1] < r[2] <= self.window[2]])
        elif sql.startswith("DESCRIBE HISTORY"):
            version = len(self.snapshots)
            self.snapshots[version] = self.rows()
            return Result([dict(version=version)])
        elif sql.startswith("ALTER TABLE dim SET"):
            name, value = re.search(r"\('([^']+)' = '([^']+)'\)", sql).groups()
            self.properties[name] = value
        elif sql.startswith("INSERT OVERWRITE"):
            version = int(sql.rsplit(" ", 1)[1])
            self.db.execute("DELETE FROM dim")
            self.db.executemany("INSERT INTO dim VALUES (?,?,?,?,?,?)", self.snapshots[version])
        elif sql.startswith("MERGE"):
            assert "MIN(`valid_from`)" in sql and f"t.`valid_to` = '{END_OF_TIME}'" in sql
            self.db.execute("""UPDATE dim SET valid_to = (SELECT min(valid_from) FROM g08_transaction_type_stage s
                            WHERE s.wwi_transaction_type_id = dim.wwi_transaction_type_id)
                            WHERE valid_to = ? AND wwi_transaction_type_id IN
                            (SELECT wwi_transaction_type_id FROM g08_transaction_type_stage)""", (END_OF_TIME,))
        elif sql.startswith("INSERT INTO dim"):
            if self.fail_insert:
                raise RuntimeError("insert interrupted")
            self.db.execute(sql)
        elif sql.startswith("SELECT"):
            return Result(self.db.execute(sql).fetchall())
        return Result()


@pytest.fixture
def spark(monkeypatch):
    class Control:
        def __init__(self, spark, catalog, schema):
            pass

        def start_lineage(self, entity, cutoff):
            assert entity == "Transaction Type"
            return 20 if cutoff == NEW else 33

    monkeypatch.setattr(job, "Integration", Control)
    return Spark()


def run(spark, drop="D1", last=LAST, new=NEW):
    job.run(spark, CONTRACT, drop=drop, last_cutoff=last, new_cutoff=new, **ARGS)


def test_baseline_and_empty_drops_replay(spark):
    run(spark, "D0")
    baseline = spark.rows()
    assert len(baseline) == 15 and baseline[0][0:3] == (0, 0, "Unknown")
    for drop, last, new in (("D1", LAST, NEW), ("D1", LAST, NEW), ("D2", NEW, NEXT), ("D2", NEW, NEXT)):
        spark.window = (drop, last, new)
        run(spark, drop, last, new)
        assert spark.rows() == baseline
    for drop in ("D0", "D1", "D2"):
        with (ROOT / f"golden/outputs/{drop}/tables/Dimension.Transaction_Type.csv").open() as stream:
            assert list(csv.reader(stream))[1:] == [[str(v) for v in row] for row in baseline]
    run(spark, "D0")
    assert spark.properties == {}


def test_all_versions_keys_minimum_close_and_partial_replay(spark):
    run(spark, "D0")
    a, b = "2016-06-07 01:00:00.0000001", "2016-06-07 02:00:00.0000002"
    spark.stage = [(1, "Customer Invoice", b, END_OF_TIME), (2, "Changed", a, END_OF_TIME),
                   (1, "Customer Invoice", a, b)]
    spark.fail_insert = True
    with pytest.raises(RuntimeError, match="insert interrupted"):
        run(spark)
    checkpoint = json.loads(spark.properties["g08.D1"])
    assert (checkpoint["high_water_mark"], checkpoint["boundary"]) == (14, 17)
    spark.fail_insert = False
    run(spark)
    rows = spark.rows()
    assert rows[1][4] == rows[2][4] == a
    assert rows[-3:] == [(15, 1, "Customer Invoice", a, b, 20),
                         (16, 2, "Changed", a, END_OF_TIME, 20),
                         (17, 1, "Customer Invoice", b, END_OF_TIME, 20)]
    run(spark)
    assert spark.rows() == rows
    spark.stage = [(1, "Next", "2016-06-08 01:00:00.0000003", END_OF_TIME)]
    spark.window = ("D2", NEW, NEXT)
    run(spark, "D2", NEW, NEXT)
    assert spark.rows()[-1][0] == 18 and spark.rows()[-1][-1] == 33
    final = spark.rows()
    run(spark, "D2", NEW, NEXT)
    assert spark.rows() == final
    spark.window = ("D1", LAST, NEW)
    with pytest.raises(ValueError, match="preceding drop"):
        run(spark)


def test_invalid_inputs_stale_silver_and_changed_replay(spark):
    for kwargs in (dict(drop="D3"), dict(last="invalid"), dict(last=NEW, new=LAST)):
        with pytest.raises(ValueError):
            run(spark, **kwargs)
    assert spark.statements == []
    run(spark, "D0")
    spark.window = ("D0", LAST, NEW)
    with pytest.raises(ValueError, match="silver"):
        run(spark)
    spark.window = ("D2", NEW, NEXT)
    with pytest.raises(ValueError, match="preceding drop"):
        run(spark, "D2", NEW, NEXT)
    spark.window = ("D1", LAST, NEW)
    run(spark)
    spark.stage = [(1, "Changed", NEW, END_OF_TIME)]
    with pytest.raises(ValueError, match="immutable stage"):
        run(spark)


class ParitySpark:
    def __init__(self, counts=(15, 15, 0, 0, 0)):
        self.counts, self.statements = counts, []

    def sql(self, sql):
        self.statements.append(sql)
        if sql.startswith("DESCRIBE"):
            return Result([(c, column_type(c)) for c in ENTRY["columns"]])
        return Result([self.counts])


@pytest.mark.parametrize("drop", ["D0", "D1", "D2"])
def test_parity(drop):
    spark = ParitySpark()
    args = {k: v for k, v in ARGS.items() if k != "silver_schema"}
    assert parity(spark, CONTRACT, drop=drop, **args)["status"] == "PASS"
    assert f"outputs/{drop}/tables/Dimension.Transaction_Type.csv" in spark.statements[-1]
    assert spark.statements[-1].count("EXCEPT ALL") == 2
    for counts in ((0, 0, 0, 0, 0), (15, 15, 1, 1, 0)):
        with pytest.raises(RuntimeError, match="parity failed"):
            parity(ParitySpark(counts), CONTRACT, drop=drop, **args)


def test_storage_errors(spark):
    def broken(statement):
        raise RuntimeError("storage unavailable")
    spark.sql = broken
    with pytest.raises(RuntimeError, match="storage unavailable"):
        run(spark, "D0")
    with pytest.raises(RuntimeError, match="storage unavailable"):
        parity(spark, CONTRACT, drop="D0", **{k: v for k, v in ARGS.items() if k != "silver_schema"})


def test_registration_and_job():
    from classify_changes import classify
    from registry import load
    registry = load(ROOT / "src/sql/units/registry.json", ROOT / "src/parity/contract.json")
    name = "wwi_dim_transaction_type_to_gold"
    assert registry[name] == dict(job=name, outputs=["gold.dim_transaction_type"], owns=[],
                                 depends_on=["p5_gold_seed", "wwi_transaction_type_to_silver"])
    base = {k: v for k, v in registry.items() if k != name}
    selected = classify(registry, ["src/sql/units/registry.json"], base)
    assert selected == ["bronze_landing_replay", "silver_temporal_views", "p5_gold_seed", "wwi_transaction_type_to_silver", name]
    resource = yaml.safe_load((ROOT / f"resources/{name}.job.yml").read_text())["resources"]["jobs"][name]
    assert "permissions" not in resource
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    for target in ("dev", "prod"):
        assert dict(group_name="users", level="CAN_VIEW") in bundle["targets"][target]["permissions"]
    assert {p["name"] for p in resource["parameters"]} == {"drop", "last_cutoff", "new_cutoff"}
    assert [t["task_key"] for t in resource["tasks"]] == ["silver", "load", "parity"]
    assert resource["tasks"][0]["run_job_task"]["job_id"] == "${resources.jobs.wwi_transaction_type_to_silver.id}"
    assert resource["tasks"][1]["depends_on"] == [dict(task_key="silver")]
    assert resource["tasks"][2]["depends_on"] == [dict(task_key="load")]
