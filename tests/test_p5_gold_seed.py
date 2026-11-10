import csv
import json
import re
from pathlib import Path

import pytest

from sql.units.p5_gold_seed.integration import ENTITIES, Integration, allocate_keys, cutoff_literal
from sql.units.p5_gold_seed.job import run
from sql.units.p5_gold_seed.seed import column_type, golden_csv_source, seed_statements

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "src/parity/contract.json").read_text())


class Result:
    def __init__(self, rows=()):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class ControlSpark:
    def __init__(self):
        self.rows = list(csv.DictReader((ROOT / "golden/outputs/D0/tables/Integration.Lineage.csv").open()))
        self.cutoffs = {r["table_name"]: r["cutoff_time"] for r in csv.DictReader(
            (ROOT / "golden/outputs/D0/tables/Integration.ETL_Cutoff.csv").open())}
        self.sqls = []

    def sql(self, sql):
        self.sqls.append(sql)
        if sql.startswith("SELECT lineage_key"):
            cutoff = sql.split(" = '")[-1][:-1]
            return Result([(r["lineage_key"], r["table_name"]) for r in self.rows if r["source_system_cutoff_time"] == cutoff])
        if sql.startswith("SELECT coalesce"):
            return Result([(max(int(r["lineage_key"]) for r in self.rows),)])
        if sql.startswith("SELECT cutoff_time"):
            return Result([(self.cutoffs[sql.split(" = '")[-1][:-1]],)])
        if sql.startswith("INSERT"):
            cutoff = re.search(r"0, '([^']+)' FROM VALUES", sql)[1]
            for key, entity in re.findall(r"\((\d+), '([^']+)'\)", sql):
                self.rows.append(dict(lineage_key=key, table_name=entity, source_system_cutoff_time=cutoff, was_successful="0"))
        elif "SET data_load_completed" in sql:
            key = re.search(r"WHERE lineage_key = (\d+)", sql)[1]
            next(r for r in self.rows if r["lineage_key"] == key)["was_successful"] = "1"
        elif "SET cutoff_time" in sql:
            cutoff = re.search(r"SET cutoff_time = '([^']+)'", sql)[1]
            self.cutoffs.update({e: cutoff for e in ENTITIES})
        return Result()


def test_seed_all_tables_and_types():
    statements = list(seed_statements(CONTRACT, "dev", "gold", "/golden", replace=True))
    assert len(statements) == 16
    assert all("outputs/D0/tables/" in s and "CREATE OR REPLACE" in s for s in statements)
    assert not any("report_" in s for s in statements)
    assert "DECIMAL(18,3)" in "\n".join(statements)
    assert "Invalid datetime2(7)" in statements[0]
    assert "nullValue=>'\\\\N'" in statements[0]
    assert column_type("date_key") == "DATE"
    assert column_type("postal_code") == "STRING"
    assert column_type("photo") == "STRING"
    assert list(seed_statements(CONTRACT, "dev", "gold", "/golden", tables=[])) == []
    date = list(seed_statements(CONTRACT, "dev", "gold", "/golden", tables=["gold.dim_date"]))
    assert len(date) == 1 and "IF NOT EXISTS" in date[0]
    entry = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_date" and e["drop"] == "D0")
    rows = list(csv.DictReader((ROOT / "golden" / entry["path"]).open()))
    assert len(entry["columns"]) == 14 and len(rows) == 1461
    assert min(r["date"] for r in rows) == "2013-01-01"
    assert max(r["date"] for r in rows) == "2016-12-31"


def test_seed_uses_parity_csv_reader_for_embedded_quotes():
    entry = next(e for e in CONTRACT["entries"] if e["table"] == "gold.dim_stock_item" and e["drop"] == "D0")
    rows = list(csv.DictReader((ROOT / "golden" / entry["path"]).open()))
    quoted = '"The Gu" red shirt XML tag t-shirt (Black) M'
    assert any(r["stock_item"] == quoted for r in rows)
    statement, = seed_statements(CONTRACT, "dev", "gold", "/golden", tables=[entry["table"]])
    source = golden_csv_source(entry, "/golden")
    assert statement.endswith("FROM " + source)
    assert 'escape=>\'"\'' in source
    assert "multiLine=>true" in source


def test_replay_keys_cutoffs_and_retries_match_golden():
    spark = ControlSpark()
    last = spark.cutoffs["City"]
    for drop, cutoff in [("D1", "2016-06-14 00:00:00.0000000"), ("D2", "2016-06-21 00:00:00.0000000")]:
        args = dict(catalog="dev", gold_schema="gold", golden_path="/golden", drop=drop, last_cutoff=last, new_cutoff=cutoff)
        run(spark, CONTRACT, **args)
        count = len(spark.rows)
        run(spark, CONTRACT, **args)
        assert len(spark.rows) == count
        expected = list(csv.DictReader((ROOT / f"golden/outputs/{drop}/tables/Integration.Lineage.csv").open()))
        for actual, golden in zip(spark.rows, expected):
            assert {c: actual[c] for c in ("lineage_key", "table_name", "was_successful", "source_system_cutoff_time")} == {c: golden[c] for c in ("lineage_key", "table_name", "was_successful", "source_system_cutoff_time")}
        assert count == len(expected)
        assert spark.cutoffs["Date"] == "2012-12-31 00:00:00.0000000"
        assert all(spark.cutoffs[e] == cutoff for e in ENTITIES)
        last = cutoff
    assert Integration(spark, "dev", "gold").start_lineage("City", last) == 27
    assert "greatest(data_load_started" in "\n".join(spark.sqls)


def test_allocations_empty_errors_and_invalid_replay():
    keys, hwm = allocate_keys(212, 1)
    assert list(keys) == [213] and hwm == 213
    keys, hwm = allocate_keys(213, 0)
    assert list(keys) == [] and hwm == 213
    for args in [(-1, 0), (0, -1), (True, 1)]:
        with pytest.raises(ValueError):
            allocate_keys(*args)
    for value in ["bad", "2016-02-30 00:00:00.0000000", None]:
        with pytest.raises(ValueError):
            cutoff_literal(value)
    with pytest.raises(ValueError):
        list(seed_statements(CONTRACT, "dev", "gold", "/golden", tables=["gold.missing"]))
    control = Integration(ControlSpark(), "dev", "gold")
    with pytest.raises(ValueError):
        control.start_lineage("Date", "2016-06-14 00:00:00.0000000")
    with pytest.raises(ValueError):
        control.complete_lineage(0)
    with pytest.raises(ValueError, match="preceding drop"):
        run(control.spark, CONTRACT, catalog="dev", gold_schema="gold", golden_path="/golden", drop="D2", last_cutoff="2016-06-14 00:00:00.0000000", new_cutoff="2016-06-21 00:00:00.0000000")
