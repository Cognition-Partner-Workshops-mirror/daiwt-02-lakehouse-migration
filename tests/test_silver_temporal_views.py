import json
import re
from collections import defaultdict
from pathlib import Path

import pytest
import yaml

from bronze.bronze_landing_replay import parse_csv, reference_replay, deleted_path
from classify_changes import classify
from silver import temporal_views as temporal

ROOT = Path(__file__).resolve().parents[1]
SPEC = json.loads((ROOT / "src/bronze/tables.json").read_text())
TABLES = temporal.temporal_tables(SPEC)


@pytest.fixture(scope="module")
def histories():
    state = {}
    drops = {}
    paired = {t["table"] for t in TABLES}
    for drop in ("D0", "D1", "D2"):
        for table in SPEC["tables"]:
            name = table["table"]
            if name.removesuffix("_archive") not in paired:
                continue
            columns = [c["name"] for c in table["columns"]]
            path = ROOT / "golden/inputs" / drop / table["file"]
            rows = parse_csv(path.read_text(), columns)
            deleted = Path(deleted_path(str(path)))
            deletes = parse_csv(deleted.read_text(), table["primary_key"] or columns) if deleted.exists() else []
            state[name] = rows if drop == "D0" else reference_replay(table, state[name], rows, deletes)
        history = {}
        for table in TABLES:
            name = table["table"]
            cols = [c["name"] for c in table["columns"]]
            history[name] = [dict(zip(cols, row), valid_from=row[cols.index("ValidFrom")],
                                  valid_to=row[cols.index("ValidTo")], _version_source=source)
                             for suffix, source in (("", "current"), ("_archive", "archive"))
                             for row in state[name + suffix]]
        drops[drop] = history
    return drops


def asof(rows, t):
    if not isinstance(t, str) or not re.fullmatch(temporal.TIME_PATTERN, t):
        raise ValueError("invalid time")
    return [r for r in rows if r["valid_from"] <= t and r["valid_to"] > t]


@pytest.mark.parametrize("drop", ("D0", "D1", "D2"))
@pytest.mark.parametrize("table", TABLES, ids=lambda t: t["table"])
def test_golden_intervals_and_asof(histories, drop, table):
    rows = histories[drop][table["table"]]
    groups = defaultdict(list)
    for row in rows:
        assert re.fullmatch(temporal.TIME_PATTERN, row["valid_from"])
        assert re.fullmatch(temporal.TIME_PATTERN, row["valid_to"])
        assert row["valid_from"] <= row["valid_to"]
        if row["valid_from"] == row["valid_to"]:
            assert row not in asof([row], row["valid_from"])
            continue
        groups[tuple(row[k] for k in table["primary_key"])].append(row)
    for versions in groups.values():
        versions.sort(key=lambda r: r["valid_from"])
        assert all(a["valid_to"] <= b["valid_from"] for a, b in zip(versions, versions[1:]))
        for t in temporal.SAMPLE_TIMES:
            assert len(asof(versions, t)) <= 1
        for version in versions:
            assert version in asof(versions, version["valid_from"])
            assert version not in asof(versions, version["valid_to"])


@pytest.mark.parametrize("drop", ("D0", "D1", "D2"))
def test_known_rows(histories, drop):
    for name, key, value, t, start, source in temporal.known_checks(drop):
        rows = [r for r in asof(histories[drop][name], t) if r[key] == str(value)]
        assert len(rows) == 1
        assert (rows[0]["valid_from"], rows[0]["_version_source"]) == (start, source)
    new = [r for r in histories[drop]["application_people"]
           if r["PersonID"] == "11" and r["valid_from"] == temporal.SAMPLE_TIMES[2]]
    assert bool(new) == (drop == "D2")


def test_generated_sql():
    sql = list(temporal.statements(SPEC, "cat", "bronze_test", "silver_test"))
    assert len(TABLES) == 14 and len(sql) == 30
    for i, table in enumerate(TABLES):
        name = table["table"]
        view, function = sql[i * 2:i * 2 + 2]
        assert view.startswith(f"CREATE OR REPLACE VIEW `cat`.`silver_test`.`{name}_history`")
        assert f"FROM `cat`.`bronze_test`.`{name}`" in view
        assert f"FROM `cat`.`bronze_test`.`{name}_archive`" in view
        assert " UNION ALL " in view
        assert "ValidFrom_str AS valid_from, ValidTo_str AS valid_to" in view
        assert "'current' AS _version_source" in view and "'archive' AS _version_source" in view
        assert f"`{name}_asof`(t STRING) RETURNS TABLE" in function
        assert "WHERE valid_from <= CASE WHEN t IS NOT NULL" in function
        assert "AND valid_to > t" in function and "raise_error" in function
    assert "SELECT DISTINCT source_table, valid_from" in sql[-2]
    assert all(f"'{t['table']}' AS source_table" in sql[-2] for t in TABLES)
    assert "array_contains(tables, source_table)" in sql[-1]
    assert "valid_from > CASE WHEN last_cutoff" in sql[-1]
    assert "valid_from <= CASE WHEN new_cutoff" in sql[-1]
    assert sql[-1].endswith("ORDER BY valid_from")
    assert len(list(temporal.invariant_queries(SPEC, "`cat`.`silver`"))) == 56


def test_empty_and_invalid_inputs():
    assert asof([], temporal.SAMPLE_TIMES[0]) == []
    for value in (None, "", "2016-06-14 08:00:00.000000", "2016-06-14T08:00:00.0000000"):
        with pytest.raises(ValueError):
            asof([], value)
    with pytest.raises(ValueError):
        list(temporal.statements({"tables": []}, "cat", "bronze", "silver"))
    with pytest.raises(ValueError):
        temporal.known_checks("D3")
    with pytest.raises(ValueError):
        list(temporal.statements(SPEC, "bad.catalog", "bronze", "silver"))


def test_change_event_windows(histories):
    for history in histories.values():
        events = {(table, r["valid_from"]) for table, rows in history.items() for r in rows}
        def changes(tables, last, new):
            return sorted({t for name, t in events if name in tables and last < t <= new})
        t = temporal.SAMPLE_TIMES[2]
        assert changes([], temporal.SAMPLE_TIMES[0], t) == []
        assert changes(["missing"], temporal.SAMPLE_TIMES[0], t) == []
        assert changes(list(history), t, t) == []
        result = changes(["application_people"], temporal.SAMPLE_TIMES[1], t)
        assert result == ([t] if history is histories["D2"] else [])


def test_registry_job_and_classification():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    entry = registry["silver_temporal_views"]
    assert entry["owns"] == temporal.owned_objects(SPEC)
    assert len(entry["owns"]) == 30
    assert entry["depends_on"] == ["bronze_landing_replay"] and entry["outputs"] == []
    for path in ("src/silver/temporal_views.py", "resources/silver_temporal_views.job.yml"):
        assert classify(registry, [path]) == ["silver_temporal_views"]
    assert classify(registry, ["resources/bronze_landing_replay.job.yml"]) == list(registry)
    job = yaml.safe_load((ROOT / "resources/silver_temporal_views.job.yml").read_text())["resources"]["jobs"][entry["job"]]
    assert job["name"] == "${var.job_name_prefix}silver_temporal_views"
    assert [p["name"] for p in job["parameters"]] == ["drop", "last_cutoff", "new_cutoff"]
    task = job["tasks"][0]
    assert task["environment_key"] == job["environments"][0]["environment_key"]
    assert job["environments"][0]["spec"]["environment_version"] == "2"
    assert task["spark_python_task"]["python_file"] == "../src/silver/temporal_views.py"
    params = task["spark_python_task"]["parameters"]
    for value in ("${var.catalog}", "${var.bronze_schema}", "${var.silver_schema}",
                  "${workspace.file_path}/src/bronze/tables.json"):
        assert value in params
    for name in ("drop", "last_cutoff", "new_cutoff"):
        assert "{{job.parameters." + name + "}}" in params
