import csv
import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from parity.parity_check import compare
from run_units import run_units
from registry import load
from classify_changes import expand
from bronze.bronze_landing_replay import Replay, reference_replay


ROOT = Path(__file__).resolve().parents[1]
UNIT = "wwi_employee_to_silver"
COLUMNS = ["wwi_employee_id", "employee", "preferred_name", "is_salesperson", "photo", "valid_from", "valid_to"]
END = "9999-12-31 23:59:59.9999999"


def rows(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def apply_snapshot(current, upserts, deletes, primary_key):
    if not (current or upserts or deletes):
        return []
    names = list((current or upserts or deletes)[0])
    table = {"columns": [{"name": name} for name in names],
             "primary_key": [primary_key] if primary_key else []}
    state = reference_replay(table, [[r[n] for n in names] for r in current],
                             [[r[n] for n in names] for r in upserts],
                             [[r[primary_key]] if primary_key else [r[n] for n in names] for r in deletes])
    return [dict(zip(names, row)) for row in state]


def typed_person(row):
    return {**row, "PersonID": int(row["PersonID"]), "IsEmployee": bool(int(row["IsEmployee"])),
            "IsSalesperson": bool(int(row["IsSalesperson"])),
            "Photo": None if row["Photo"] == "\\N" else row["Photo"],
            "ValidFrom": datetime.fromisoformat(row["ValidFrom"][:26]), "ValidFrom_str": row["ValidFrom"],
            "ValidTo": datetime.fromisoformat(row["ValidTo"][:26]), "ValidTo_str": row["ValidTo"]}


def extract(history, last, new):
    events = [(r["PersonID"], r["ValidFrom_str"]) for r in history
              if last < r["ValidFrom_str"] <= new and r["IsEmployee"]]
    staged = []
    for person, instant in events:
        for row in history:
            if row["PersonID"] == person and row["ValidFrom_str"] <= instant < row["ValidTo_str"]:
                staged.append((str(row["PersonID"]), row["FullName"], row["PreferredName"],
                               str(int(row["IsSalesperson"])),
                               row["Photo"].upper() if row["Photo"] is not None else "\\N", row["ValidFrom_str"]))
    return Counter(r + (min((n[5] for n in staged if n[0] == r[0] and n[5] > r[5]), default=END),)
                   for r in staged)


def cutoff(value):
    if isinstance(value, list):
        value = next(r["Cutoff Time"] for r in value if r["Table Name"] == "Employee")
    return value if "." in value else value + ".0000000"


def employee_window(windows, log, table_count):
    complete = []
    for drop in {r["drop"] for r in log}:
        records = [r for r in log if r["drop"] == drop]
        if (len(records) == table_count and all(r["status"] == "PASS" for r in records)
                and len({r["source_table"] for r in records}) == table_count):
            complete.append(drop)
    if not complete:
        raise ValueError("No fully applied drop")
    return next(w for w in windows if w["name"] == max(complete, key=lambda d: int(d[1:])))


def test_golden_employee_replay():
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    spec = json.loads((ROOT / "src/bronze/tables.json").read_text())
    log = []
    snapshots = {"People": [], "People_Archive": []}
    previous_keys = set()
    counts = []
    for window in manifest["drops"]:
        drop = window["name"]
        for source in snapshots:
            path = f"inputs/{drop}/WideWorldImporters.Application.{source}.csv"
            deleted = path.removesuffix(".csv") + ".deleted.csv"
            snapshots[source] = apply_snapshot(
                snapshots[source], rows(ROOT / "golden" / path),
                rows(ROOT / "golden" / deleted) if deleted in manifest["files"] else [],
                "PersonID" if source == "People" else None)
            assert len(snapshots[source]) == manifest["files"][path]["full_row_count"]
        output = rows(ROOT / f"golden/outputs/{drop}/tables/Dimension.Employee.csv")
        expected = Counter(tuple(r[c] for c in COLUMNS) for r in output
                           if drop != "D0" and r["employee_key"] not in previous_keys)
        log.extend({"drop": drop, "status": "PASS", "source_table": t["source_table"]} for t in spec["tables"])
        applied = employee_window(manifest["drops"], log, len(spec["tables"]))
        actual = extract([typed_person(r) for r in snapshots["People"] + snapshots["People_Archive"]],
                         cutoff(applied["last_cutoff"]), cutoff(applied["new_cutoff"]))
        assert actual == expected
        counts.append(sum(actual.values()))
        previous_keys = {r["employee_key"] for r in output}
    assert counts == [0, 0, 1]
    assert actual == Counter({("11", "Ethan Onslow", "Ethan", "0", "\\N",
                               "2016-06-14 08:00:00.0000000", END): 1})


def person(**changes):
    return {"PersonID": "1", "FullName": "A", "PreferredName": "", "IsEmployee": "1",
            "IsSalesperson": "0", "Photo": "ab01", "ValidFrom": "2016-01-01 00:00:00.0000001",
            "ValidTo": END, **changes}


def test_reference_boundaries_duplicates_and_strictly_later_valid_to():
    first = person(ValidTo="2016-01-01 00:00:00.0000002")
    second = person(ValidFrom=first["ValidTo"])
    assert typed_person(first)["ValidFrom"] == typed_person(second)["ValidFrom"]
    result = extract([typed_person(r) for r in [first, first, second]],
                     "2016-01-01 00:00:00.0000000", second["ValidFrom"])
    assert sum(result.values()) == 5
    assert result[("1", "A", "", "0", "AB01", first["ValidFrom"], second["ValidFrom"])] == 4
    assert extract([typed_person(first)], first["ValidFrom"], first["ValidFrom"]) == Counter()
    assert extract([], first["ValidFrom"], END) == Counter()
    assert extract([typed_person(person(IsEmployee="0"))], "2015", END) == Counter()


def test_reference_deletes_and_upserts():
    a = person()
    b = person(FullName="B")
    assert apply_snapshot([a, a], [], [a], None) == [a]
    assert apply_snapshot([a], [b], [], "PersonID") == [b]
    assert apply_snapshot([a], [], [{"PersonID": "1"}], "PersonID") == []
    for key in (None, "PersonID"):
        with pytest.raises(ValueError, match="absent"):
            apply_snapshot([], [], [a], key)


def test_latest_fully_applied_window():
    windows = [{"name": f"D{i}"} for i in range(3)]
    log = [{"drop": "D0", "source_table": str(i), "status": "PASS"} for i in range(38)]
    log += [{"drop": "D1", "source_table": "0", "status": "PASS"}]
    assert employee_window(windows, log, 38)["name"] == "D0"
    log += [{"drop": "D1", "source_table": str(i), "status": "PASS"} for i in range(1, 38)]
    assert employee_window(windows, log, 38)["name"] == "D1"
    log[-1]["status"] = "FAIL"
    assert employee_window(windows, log, 38)["name"] == "D0"
    with pytest.raises(ValueError, match="No fully applied"):
        employee_window(windows, [], 38)


def test_p4_never_twice_and_drop_order_guards():
    replay = Replay.__new__(Replay)
    replay.spec = {"tables": [None] * 38}
    replay.prefix = "bronze"
    replay.manifest = {"drops": [{"name": f"D{i}"} for i in range(3)]}
    applied = [{"drop": "D0", "passed": 38, "n": 38}]
    replay.sql = lambda _: SimpleNamespace(collect=lambda: applied)
    replay.drop = "D0"
    replay.guard()
    replay.drop = "D1"
    replay.guard()
    applied.append({"drop": "D1", "passed": 38, "n": 38})
    with pytest.raises(ValueError, match="already started"):
        replay.guard()
    replay.drop = "D2"
    applied[-1]["passed"] = 37
    with pytest.raises(ValueError, match="last fully applied"):
        replay.guard()


def test_employee_p4_sql_contract():
    sql = (ROOT / f"src/sql/units/{UNIT}/employee.sql").read_text()
    assert "person_as_of AS (" in sql
    assert "p.ValidFrom_str <= e.ValidFrom_str AND p.ValidTo_str > e.ValidFrom_str" in sql
    assert "p.IsEmployee = true" in sql and "cast(IsSalesperson AS INT)" in sql
    assert "employee_drop_window" not in sql and "_loaded_drop" not in sql
    assert "bronze_replay_log" in sql and "golden_drops" in sql
    spec = json.loads((ROOT / "src/bronze/tables.json").read_text())
    assert len(spec["tables"]) == 38
    for table in spec["tables"]:
        if table["table"] in ("application_people", "application_people_archive"):
            types = {c["name"]: c["type"] for c in table["columns"]}
            assert types["IsEmployee"] == types["IsSalesperson"] == "BOOLEAN"
            assert types["ValidFrom"] == types["ValidTo"] == "TIMESTAMP_NTZ"


def test_employee_bundle_registry_and_harness(monkeypatch):
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["include"] == ["resources/*.yml"]
    resource = yaml.safe_load((ROOT / f"resources/{UNIT}.yml").read_text())["resources"]
    job = resource["jobs"][UNIT]
    assert job["name"] == "${var.job_name_prefix}" + UNIT
    assert {p["name"]: p["default"] for p in job["parameters"]} == {
        "drop": "${var.drop}", "last_cutoff": "${var.last_cutoff}", "new_cutoff": "${var.new_cutoff}"}
    assert job["permissions"] == [{"group_name": "users", "level": "CAN_VIEW"}]
    tasks = job["tasks"]
    assert [t["task_key"] for t in tasks] == ["silver", "parity"]
    assert "depends_on" not in tasks[0]
    assert tasks[0]["pipeline_task"]["pipeline_id"] == "${resources.pipelines.wwi_employee_pipeline.id}"
    assert tasks[1]["depends_on"] == [{"task_key": "silver"}]
    assert tasks[1]["sql_task"]["warehouse_id"] == "${var.warehouse_id}"
    assert tasks[1]["sql_task"]["parameters"]["drop"] == "{{job.parameters.drop}}"
    assert all("run_job_task" not in t and "notebook_task" not in t for t in tasks)
    p4 = yaml.safe_load((ROOT / "resources/bronze_landing_replay.job.yml").read_text())
    assert "bronze_landing_replay" in p4["resources"]["jobs"]
    assert not (ROOT / f"src/sql/units/{UNIT}/bronze.py").exists()
    pipeline = resource["pipelines"]["wwi_employee_pipeline"]
    assert pipeline["serverless"] is True
    assert pipeline["permissions"] == [{"group_name": "users", "level": "CAN_VIEW"}]
    assert pipeline["catalog"] == "${var.catalog}"
    assert pipeline["target"] == "${var.silver_schema}"
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    spec = registry[UNIT]
    assert spec["job"] == UNIT and spec["outputs"] == []
    assert spec["depends_on"] == ["bronze_landing_replay"]
    assert set(spec["owns"]) == {
        "silver.employee_changes", "silver.employee_changes_quarantine"}
    assert load()[UNIT] == spec
    selected = expand(registry, ["bronze_landing_replay", UNIT])
    assert selected == ["bronze_landing_replay", UNIT]
    calls = []

    class Process:
        stdout = ["Run URL: https://example.test/run\\n"]

        def __init__(self, command, **kwargs):
            calls.append(command)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def wait(self):
            return 0

    monkeypatch.setattr("subprocess.Popen", Process)
    run_units("pr_999", registry, selected, drop="D2")
    params = "drop=D2,last_cutoff=2016-06-14 00:00:00.0000000,new_cutoff=2016-06-21 00:00:00.0000000"
    assert calls == [["databricks", "bundle", "run", "-t", "dev", "--var",
                      "schema_suffix=pr_999", "--params", params, registry[u]["job"]] for u in selected]
    parity = (ROOT / f"src/sql/units/{UNIT}/parity.sql").read_text()
    assert parity.count("EXCEPT ALL") == 2
    assert "WHERE :drop <> 'D0'" in parity
    assert "employee_changes_quarantine" in parity and "raise_error" in parity
    assert "replay window does not match" in parity
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    result = compare(manifest, {UNIT: spec}, lambda _: pytest.fail("unexpected golden read"),
                     lambda _: pytest.fail("unexpected output read"))
    assert result["status"] == "PASS"
    assert all(r["status"] == "SKIP" for r in result["results"])
