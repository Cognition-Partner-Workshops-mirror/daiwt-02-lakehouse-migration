import json
import re
import hashlib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT / "src/bronze"))
import bronze_landing_replay as bronze  # noqa: E402
import build_spec  # noqa: E402

MANIFEST = json.loads((ROOT / "golden/manifest.json").read_text())
SPEC = json.loads((ROOT / "src/bronze/tables.json").read_text())
DROPS = [d["name"] for d in MANIFEST["drops"]]
LEGACY = ROOT.parent / "daiwt-02-enterprise-etl"


def golden_rows(relative, columns):
    return bronze.parse_csv((ROOT / "golden" / relative).read_text(encoding="utf-8"), columns)


def test_spec_covers_every_golden_input():
    tables = SPEC["tables"]
    assert len(tables) == 38
    assert sum(1 for t in tables if t["primary_key"]) == 24
    assert sum(1 for t in tables if t["key_semantics"] == "whole_row_multiset") == 14
    for t in tables:
        for drop in DROPS:
            meta = MANIFEST["files"][f"inputs/{drop}/{t['file']}"]
            assert meta["columns"] == [c["name"] for c in t["columns"]]
            assert meta["primary_key"] == t["primary_key"]
            assert meta["key_semantics"] == t["key_semantics"]
        assert t["table"] == build_spec.bronze_name(t["source_table"])
        assert all(c["type"] for c in t["columns"])


def test_bronze_names_match_target_model():
    model = (ROOT / "docs/wave0/02-target-model.md").read_text()
    documented = set(re.findall(r"^\| bronze \| bronze\.(\w+) \|", model, re.M))
    assert documented == {t["table"] for t in SPEC["tables"]}


@pytest.mark.skipif(not LEGACY.is_dir(), reason="legacy checkout not available")
def test_spec_regenerates_from_legacy_ddl():
    assert build_spec.build(MANIFEST, LEGACY)["tables"] == SPEC["tables"]


def test_registry_and_job():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    entry = registry["bronze_landing_replay"]
    assert entry["outputs"] == [] and entry["depends_on"] == []
    assert entry["owns"] == bronze.owned_tables(SPEC)
    assert len(entry["owns"]) == 78
    job = yaml.safe_load((ROOT / "resources/bronze_landing_replay.job.yml").read_text())
    job = job["resources"]["jobs"][entry["job"]]
    assert job["name"] == "${var.job_name_prefix}bronze_landing_replay"
    assert [p["name"] for p in job["parameters"]] == ["drop", "last_cutoff", "new_cutoff"]
    params = job["tasks"][0]["spark_python_task"]["parameters"]
    assert "{{job.parameters.drop}}" in params and "${var.bronze_schema}" in params


@pytest.mark.parametrize("table", SPEC["tables"], ids=lambda t: t["table"])
def test_reference_replay_reproduces_full_state(table):
    names = [c["name"] for c in table["columns"]]
    state = None
    for drop in DROPS:
        relative = f"inputs/{drop}/{table['file']}"
        meta = MANIFEST["files"][relative]
        raw = (ROOT / "golden" / relative).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == meta["sha256"]
        rows = bronze.parse_csv(raw.decode("utf-8"), names)
        assert bronze.render_csv(names, rows).encode("utf-8") == raw
        deleted = bronze.deleted_path(relative)
        deletes = golden_rows(deleted, MANIFEST["files"][deleted]["columns"]) if deleted in MANIFEST["files"] else []
        state = rows if state is None else bronze.reference_replay(table, state, rows, deletes)
        ordered = sorted(state, key=bronze.reference_sort_key(table))
        assert len(state) == meta["full_row_count"]
        assert hashlib.sha256(bronze.render_csv(names, ordered).encode("utf-8")).hexdigest() == meta["full_sha256"]


def synthetic(pk):
    return {"table": "t", "primary_key": ["Id"] if pk else [],
            "columns": [{"name": "Id", "type": "INT"}, {"name": "Name", "type": "STRING"},
                        {"name": "ValidFrom", "type": "TIMESTAMP_NTZ"}]}


def test_synthetic_primary_key_deletes_then_upserts():
    table = synthetic(True)
    state = [["1", "a", "x"], ["2", "b", "x"], ["3", "c", "x"]]
    result = bronze.reference_replay(table, state, [["3", "c2", "y"], ["4", "d", "y"]], [["2"]])
    assert sorted(result) == [["1", "a", "x"], ["3", "c2", "y"], ["4", "d", "y"]]
    with pytest.raises(ValueError):
        bronze.reference_replay(table, state, [], [["9"]])


def test_synthetic_multiset_deletes_one_copy():
    table = synthetic(False)
    row = ["1", "a", "x"]
    state = [row, row, ["1", None, "x"]]
    result = bronze.reference_replay(table, state, [row], [row])
    assert sorted(result, key=bronze.reference_sort_key(table)) == [["1", None, "x"], row, row]
    with pytest.raises(ValueError):
        bronze.reference_replay(table, state, [], [["2", "a", "x"]])


def test_header_only_drop_is_no_change():
    table = synthetic(True)
    state = [["1", "a", "x"]]
    assert bronze.reference_replay(table, state, [], []) == state


def test_sql_generation():
    pk, ms = synthetic(True), synthetic(False)
    assert bronze.typed_select(pk) == ["CAST(`Id` AS INT) AS `Id`", "CAST(`Name` AS STRING) AS `Name`",
                                       "CAST(`ValidFrom` AS TIMESTAMP_NTZ) AS `ValidFrom`",
                                       "`ValidFrom` AS `ValidFrom_str`"]
    assert bronze.pk_delete_sql("T", pk, "D").endswith("WHERE t.`Id` <=> d.`Id`)")
    assert "ON t.`Id` <=> s.`Id`" in bronze.merge_sql("T", pk, "S")
    delete = bronze.multiset_delete_sql("T", ms, "D")
    assert delete.startswith("INSERT OVERWRITE T SELECT s.`Id`, s.`Name`, s.`ValidFrom`, s.`ValidFrom_str`, s.`_drop`")
    assert "s.`ValidFrom_str` <=> d.`ValidFrom_str`" in delete and "s._rn > coalesce(d._n, 0)" in delete
    assert "coalesce(s._n, 0) < d._n" in bronze.multiset_missing_deletes_sql("T", ms, "D")
    assert bronze.golden_order(pk) == ["`Id` ASC NULLS FIRST"]
    assert bronze.golden_order(ms) == ["`Id` ASC NULLS FIRST", "lower(`Name`) ASC NULLS FIRST",
                                       "`Name` ASC NULLS FIRST", "`ValidFrom_str` ASC NULLS FIRST"]
    line = bronze.golden_line(ms)
    assert "'\\\\N'" in line and "`ValidFrom_str`" in line and "CAST(`ValidFrom` AS" not in line


def test_drop_validation():
    assert bronze.drop_index("D2") == 2
    with pytest.raises(ValueError):
        bronze.drop_index("D2; DROP")
