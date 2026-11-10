import copy
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import daily_etl_dag as dag
from drops import DROPS, drop_params
from orchestration.dev_bad_rows import exercise
from orchestration.verify_silver import golden_counts, verify
from parity import parity_check

ROOT = Path(__file__).resolve().parents[1]


def tasks(target):
    path = os.environ.get(f"P8_{target.upper()}_BUNDLE")
    if path:
        document = json.loads(Path(path).read_text())
        result = document["resources"]["jobs"]["wwi_daily_etl"]["tasks"]
    else:
        document = yaml.safe_load(dag.OUTPUT.read_text())
        result = copy.deepcopy(document["resources"]["jobs"]["wwi_daily_etl"]["tasks"])
        if target == "dev":
            for task in document["targets"]["dev"]["resources"]["jobs"]["wwi_daily_etl"]["tasks"]:
                existing = next((t for t in result if t["task_key"] == task["task_key"]), None)
                if existing:
                    existing["depends_on"].extend(task["depends_on"])
                else:
                    result.append(task)
    return {t["task_key"]: t for t in result}


@pytest.mark.parametrize("target", ["dev", "prod"])
def test_dag(target):
    graph = tasks(target)
    registry, units = dag.definitions()
    final = graph["governance"]
    assert final["run_if"] == "NONE_FAILED"
    deps = lambda key: {d["task_key"] for d in graph[key].get("depends_on", [])}
    assert {f"{d}_parity" for d in DROPS} <= deps("governance")
    assert ("dev_bad_row_quarantine" in graph) == (target == "dev")
    upstream = set()

    def visit(key, active):
        assert key not in active, "cycle"
        for parent in deps(key):
            assert parent in graph
            visit(parent, active | {key}) if parent not in upstream else None
            upstream.add(parent)

    visit("governance", set())
    assert upstream == graph.keys() - {"governance"}
    assert graph.keys() - {p for k in graph for p in deps(k)} == {"governance"}
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    for index, drop in enumerate(DROPS):
        unit_keys = {f"{drop}_{u}" for u in units if u != "governance"}
        for unit, spec in units.items():
            if unit == "governance":
                continue
            key = f"{drop}_{unit}"
            assert key in graph
            assert deps(key) & unit_keys == {f"{drop}_{d}" for d in spec["depends_on"]}
            if index and not spec["depends_on"]:
                assert f"{DROPS[index - 1]}_parity" in deps(key)
            if dag.stage(unit, spec) != "silver":
                assert f"{drop}_verify_silver" in deps(key)
                assert key in deps(f"{drop}_parity")
            else:
                assert key in deps(f"{drop}_verify_silver")
            if unit in registry:
                assert graph[key]["run_job_task"]["job_parameters"] == drop_params(manifest, drop)
            else:
                assert graph[key]["condition_task"] == dict(op="EQUAL_TO", left="1", right="1")
        assert f"{drop}_verify_silver" in deps(f"{drop}_parity")
    for task in graph.values():
        for dep in task.get("depends_on", []):
            if "condition_task" in graph[dep["task_key"]]:
                assert dep["outcome"] == "true"


def test_generated_and_settings():
    assert dag.OUTPUT.read_text() == dag.render()
    job = dag.generate()["resources"]["jobs"]["wwi_daily_etl"]
    assert job["tasks"][-1]["task_key"] == "governance"
    assert job["max_concurrent_runs"] == 1 and job["queue"]["enabled"]
    assert job["timeout_seconds"] == 7200
    assert job["schedule"] == dict(quartz_cron_expression="0 30 1 * * ?", timezone_id="UTC", pause_status="PAUSED")
    assert job["health"]["rules"] == [dict(metric="RUN_DURATION_SECONDS", op="GREATER_THAN", value=3600)]
    assert "email_notifications" not in job


def test_registry_replaces_plan(monkeypatch):
    registry, units = dag.definitions()
    registry["wwi_customer_to_silver"] = units["wwi_customer_to_silver"]
    registry["governance"] = units["governance"]
    monkeypatch.setattr(dag, "definitions", lambda: (registry, units))
    graph = {t["task_key"]: t for t in dag.generate()["resources"]["jobs"]["wwi_daily_etl"]["tasks"]}
    assert graph["D0_wwi_customer_to_silver"]["run_job_task"]["job_id"] == "${resources.jobs.wwi_customer_to_silver.id}"
    assert graph["governance"]["run_job_task"]["job_id"] == "${resources.jobs.governance.id}"


class Spark:
    def __init__(self, count=0, error=False, columns=("_drop",)):
        self.count, self.error, self.calls = count, error, []
        self.columns = columns

    def sql(self, statement):
        self.calls.append(statement)
        if self.error:
            raise RuntimeError("missing table")
        return SimpleNamespace(collect=lambda: [[self.count]], columns=self.columns)


def test_silver_normal_empty_and_failures(capsys):
    registry = {"a": dict(outputs=[], owns=["silver.customer_changes", "silver.customer_changes_quarantine", "bronze.raw"])}
    spark = Spark()
    assert verify(spark, registry, "cat", "silver_pr_1", "D0") == 2
    assert "WHERE `_drop` = 'D0'" in spark.calls[-1]
    assert verify(Spark(), {}, "cat", "silver_pr_1", "D1") == 0
    assert verify(Spark(1), registry, "cat", "silver_pr_1", "D2") == 2
    assert "QUARANTINE D2: silver.customer_changes_quarantine: 1 rows" in capsys.readouterr().out
    with pytest.raises(RuntimeError, match="missing table"):
        verify(Spark(error=True), registry, "cat", "silver_pr_1", "D0")
    with pytest.raises(ValueError, match="golden"):
        verify(Spark(), registry, "cat", "schema", "DEV_BAD")


def test_dev_guard_and_p9_requirement(monkeypatch):
    with pytest.raises(ValueError, match="dev catalog"):
        exercise(None, "daiwt_02_mig_prod", "silver")
    monkeypatch.setitem(sys.modules, "governance.quality", None)
    with pytest.raises(RuntimeError, match="merge PR #28"):
        exercise(None, "daiwt_02_mig_dev", "silver_pr_999")


@pytest.mark.parametrize("columns,expected", [(("_drop",), "_drop"), (("drop",), "drop"),
                                             (("drop", "_drop"), "_drop")])
@pytest.mark.parametrize("count", [0, 1])
def test_silver_quarantine_drop_columns(columns, expected, count, capsys):
    registry = {"a": dict(outputs=[], owns=["silver.stock_holding_snapshot_quarantine"])}
    spark = Spark(count, columns=columns)
    assert verify(spark, registry, "cat", "silver_pr_1", "D2") == 1
    assert f"WHERE `{expected}` = 'D2'" in spark.calls[-1]
    assert f"QUARANTINE D2: silver.stock_holding_snapshot_quarantine: {count} rows" in capsys.readouterr().out


def test_golden_count_mapping():
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    bronze = json.loads((ROOT / "src/bronze/tables.json").read_text())
    contract = json.loads((ROOT / "src/parity/contract.json").read_text())
    for drop in DROPS:
        counts = golden_counts(manifest, bronze, contract, drop)
        assert len(counts) == len(bronze["tables"]) == 38
        assert all(table.startswith("bronze.") for table in counts)
        assert counts["bronze.application_cities"] == manifest["files"][f"inputs/{drop}/WideWorldImporters.Application.Cities.csv"]["full_row_count"]
    assert golden_counts({"files": {}}, {"tables": []}, {"entries": []}, "D0") == {}
    assert golden_counts({"files": {}}, {"tables": []}, {"entries": [
        dict(table="silver.changes", drop="D0", row_count=2),
        dict(table="silver.changes", drop="D1", row_count=3)]}, "D0") == {"silver.changes": 2}


@pytest.mark.parametrize("layer", ["silver", "bronze"])
@pytest.mark.parametrize("actual", [0, 2])
def test_mapped_golden_count_check(layer, actual, capsys):
    table = f"{layer}.changes"
    registry = {"a": dict(outputs=[], owns=[table])}
    spark = Spark(actual)
    if actual != 2:
        with pytest.raises(RuntimeError, match="golden row-count mismatch for D1: expected 2, got 0"):
            verify(spark, registry, "cat", "silver_pr_1", "D1", expected_counts={table: 2}, bronze_schema="bronze_pr_1")
    else:
        verify(spark, registry, "cat", "silver_pr_1", "D1", expected_counts={table: 2}, bronze_schema="bronze_pr_1")
        assert f"COUNT D1: {table}: 2 rows matched golden" in capsys.readouterr().out
    assert spark.calls[-1] == f"SELECT count(*) FROM `cat`.`{layer}_pr_1`.`changes`"


def test_unmapped_counts_are_not_invented():
    spark = Spark(99)
    registry = {"a": dict(outputs=[], owns=["silver.changes", "bronze.raw"])}
    assert verify(spark, registry, "cat", "silver_pr_1", "D0", expected_counts={"bronze.unregistered": 1}) == 1
    assert spark.calls == ["SELECT * FROM `cat`.`silver_pr_1`.`changes` LIMIT 0"]


def test_silver_quarantine_missing_drop_column():
    registry = {"a": dict(outputs=[], owns=["silver.customer_changes_quarantine"])}
    with pytest.raises(RuntimeError, match="silver.customer_changes_quarantine: quarantine table requires an _drop or drop column"):
        verify(Spark(columns=("customer_id",)), registry, "cat", "silver_pr_1", "D0")


@pytest.mark.parametrize("count", [3, 0])
def test_dev_fixture_uses_p9_isolated_tables(monkeypatch, count):
    spark = Spark(count)
    writes, invocations = [], []
    writer = SimpleNamespace(mode=lambda mode: SimpleNamespace(saveAsTable=writes.append))
    spark.createDataFrame = lambda rows, schema: (invocations.append(rows) or SimpleNamespace(write=writer))

    def quarantine(source, target, rules, drop):
        invocations.append((source, target, rules, drop))
        return ["P9 quarantine statement"]

    monkeypatch.setitem(sys.modules, "governance.quality", SimpleNamespace(quarantine_sql=quarantine))
    if count:
        exercise(spark, "daiwt_02_mig_dev", "silver_pr_999")
    else:
        with pytest.raises(RuntimeError, match="Expected 3 fixture rejects"):
            exercise(spark, "daiwt_02_mig_dev", "silver_pr_999")
    assert len(invocations[0]) == 3
    assert all(row[-1] == "DEV_BAD" for row in invocations[0])
    source, target, rules, drop = invocations[1]
    assert source.endswith(".`dev_bad_rows`")
    assert target.endswith(".`dev_bad_rows_quarantine`")
    assert writes == [source] and drop == "DEV_BAD"
    assert set(rules) == {"customer_id_valid", "valid_window"}
    assert spark.calls[0] == "P9 quarantine statement"


@pytest.mark.parametrize("status,code", [("PASS", 0), ("FAIL", 1)])
def test_parity_cli_default_auth_and_failure(monkeypatch, tmp_path, status, code):
    import databricks.sdk
    calls = []
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda **kw: calls.append(kw) or object())
    monkeypatch.setattr(parity_check, "run", lambda *a, **kw: dict(status=status, compared=1, goldens=18,
                        counts=dict(PASS=int(code == 0), FAIL=code, SKIP=17)))
    output = tmp_path / "parity.json"
    monkeypatch.setattr(sys, "argv", ["parity_check.py", "--suffix", "pr_999", "--output", str(output)])
    assert parity_check.main() == code
    assert calls == [{}]
    assert json.loads(output.read_text())["status"] == status
