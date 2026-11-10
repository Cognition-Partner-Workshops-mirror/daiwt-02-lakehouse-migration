import csv
import json
from collections import Counter
from pathlib import Path

import pytest
import yaml

from parity.parity_check import compare
from run_units import run_units


ROOT = Path(__file__).resolve().parents[1]
UNIT = "wwi_employee_to_silver"
COLUMNS = ["wwi_employee_id", "employee", "preferred_name", "is_salesperson", "photo", "valid_from", "valid_to"]
END = "9999-12-31 23:59:59.9999999"


def rows(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def apply_snapshot(current, upserts, deletes, primary_key):
    current = [dict(r) for r in current]
    for row in deletes:
        match = next((r for r in current if
                      (r[primary_key] == row[primary_key] if primary_key else r == row)), None)
        if match is None:
            raise ValueError("Deleted row is absent")
        current.remove(match)
    if primary_key:
        keys = {r[primary_key] for r in upserts}
        current = [r for r in current if r[primary_key] not in keys]
    return current + upserts


def extract(history, last, new):
    events = [(r["PersonID"], r["ValidFrom"]) for r in history
              if last < r["ValidFrom"] <= new and int(r["IsEmployee"]) != 0]
    staged = []
    for person, instant in events:
        for row in history:
            if row["PersonID"] == person and row["ValidFrom"] <= instant < row["ValidTo"]:
                staged.append((row["PersonID"], row["FullName"], row["PreferredName"],
                               str(int(row["IsSalesperson"])),
                               row["Photo"].upper() if row["Photo"] != "\\N" else "\\N", row["ValidFrom"]))
    return Counter(r + (min((n[5] for n in staged if n[0] == r[0] and n[5] > r[5]), default=END),)
                   for r in staged)


def cutoff(value):
    if isinstance(value, list):
        value = next(r["Cutoff Time"] for r in value if r["Table Name"] == "Employee")
    return value if "." in value else value + ".0000000"


def test_golden_employee_replay():
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
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
        actual = extract(snapshots["People"] + snapshots["People_Archive"],
                         cutoff(window["last_cutoff"]), cutoff(window["new_cutoff"]))
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
    result = extract([first, first, second], "2016-01-01 00:00:00.0000000", second["ValidFrom"])
    assert sum(result.values()) == 5
    assert result[("1", "A", "", "0", "AB01", first["ValidFrom"], second["ValidFrom"])] == 4
    assert extract([first], first["ValidFrom"], first["ValidFrom"]) == Counter()
    assert extract([], first["ValidFrom"], END) == Counter()
    assert extract([person(IsEmployee="0")], "2015", END) == Counter()


def test_reference_deletes_and_upserts():
    a = person()
    b = person(FullName="B")
    assert apply_snapshot([a, a], [], [a], None) == [a]
    assert apply_snapshot([a], [b], [], "PersonID") == [b]
    assert apply_snapshot([a], [], [{"PersonID": "1"}], "PersonID") == []
    for key in (None, "PersonID"):
        with pytest.raises(ValueError, match="absent"):
            apply_snapshot([], [], [a], key)


def test_employee_bundle_registry_and_harness(monkeypatch):
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert bundle["include"] == ["resources/*.yml"]
    resource = yaml.safe_load((ROOT / f"resources/{UNIT}.yml").read_text())["resources"]
    job = resource["jobs"][UNIT]
    assert job["name"] == "${var.job_name_prefix}" + UNIT
    assert "parameters" not in job
    assert job["permissions"] == [{"group_name": "users", "level": "CAN_VIEW"}]
    tasks = job["tasks"]
    assert [t["task_key"] for t in tasks] == [f"{step}_D{i}" for i in range(3)
                                            for step in ("bronze", "silver", "parity")]
    for index, task in enumerate(tasks):
        assert task.get("depends_on", []) == ([{"task_key": tasks[index - 1]["task_key"]}] if index else [])
        if "sql_task" in task:
            assert task["sql_task"]["warehouse_id"] == "${var.warehouse_id}"
        if "pipeline_task" in task:
            assert task["pipeline_task"]["pipeline_id"] == "${resources.pipelines.wwi_employee_pipeline.id}"
    pipeline = resource["pipelines"]["wwi_employee_pipeline"]
    assert pipeline["serverless"] is True
    assert pipeline["catalog"] == "${var.catalog}"
    assert pipeline["target"] == "${var.silver_schema}"
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    spec = registry[UNIT]
    assert spec["job"] == UNIT and spec["outputs"] == [] and spec["depends_on"] == []
    assert set(spec["owns"]) == {
        "bronze.application_people", "bronze.application_people_raw",
        "bronze.application_people_archive", "bronze.application_people_archive_raw",
        "bronze.employee_drop_window", "silver.employee_changes", "silver.employee_changes_quarantine"}
    calls = []
    monkeypatch.setattr("subprocess.run", lambda command, **kwargs: calls.append(command))
    run_units("pr_999", registry, [UNIT])
    assert calls == [["databricks", "bundle", "run", "-t", "dev", "--var", "schema_suffix=pr_999", UNIT]]
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    result = compare(manifest, {UNIT: spec}, lambda _: pytest.fail("unexpected golden read"),
                     lambda _: pytest.fail("unexpected output read"))
    assert result["status"] == "PASS"
    assert all(r["status"] == "SKIP" for r in result["results"])
