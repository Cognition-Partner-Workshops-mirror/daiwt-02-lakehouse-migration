import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from attest import render
from classify_changes import classify
from cleanup import candidates
from run_units import run_units

ROOT = Path(__file__).resolve().parents[1]


def test_dev_runs_all_registered_units(monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda command, **kwargs: calls.append(command))
    run_units("pr_1", {"sale": {"job": "sale_job"}, "customer": {"job": "customer_job"}})
    assert [command[-1] for command in calls] == ["sale_job", "customer_job"]
    assert all(command[4:6] == ["dev", "--var"] for command in calls)
    script = (ROOT / "ci/dev_verify.sh").read_text()
    assert 'python ci/run_units.py "$suffix"\n' in script


@pytest.mark.parametrize("target,suffix", [("dev", "pr_1"), ("prod", "prod")])
def test_run_units_empty(monkeypatch, target, suffix):
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: pytest.fail("unexpected job"))
    run_units(suffix, {}, target=target)


def test_prod_runs_registered_units_before_parity(monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda command, **kwargs: calls.append(command))
    run_units("prod", {"sale": {"job": "sale_job"}, "customer": {"job": "customer_job"}}, target="prod")
    assert [command[-1] for command in calls] == ["sale_job", "customer_job"]
    assert all(command[4:7] == ["prod", "--var", "schema_suffix=prod"] for command in calls)
    workflow = yaml.safe_load((ROOT / ".github/workflows/deploy-prod.yml").read_text())
    commands = [step.get("run") for step in workflow["jobs"]["deploy"]["steps"]]
    assert commands.index("databricks bundle deploy -t prod") < commands.index(
        "python ci/run_units.py prod --target prod") < commands.index(
        "python src/parity/parity_check.py --catalog daiwt_02_mig_prod --suffix prod")


@pytest.mark.parametrize("target,suffix", [("dev", "prod"), ("prod", "pr_1"), ("other", "pr_1")])
def test_run_units_rejects_invalid_target_suffix(target, suffix):
    with pytest.raises(ValueError):
        run_units(suffix, {}, target=target)


def test_run_units_failure(monkeypatch):
    def fail(command, **kwargs):
        raise subprocess.CalledProcessError(1, command)
    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        run_units("pr_1", {"sale": {"job": "sale_job"}})


@pytest.mark.parametrize("status,ok", [("PASS", True), ("SKIP", True), ("FAIL", False), ("UNKNOWN", False)])
def test_attestation(status, ok):
    text, valid = render({"status": "PASS", "goldens": 1,
                          "results": [{"table": "gold.fact_sale", "status": status, "reason": "detail"}]})
    assert valid is ok
    assert f"| gold.fact_sale | {status} | detail |" in text


def test_attest_empty():
    assert render({"status": "PASS", "goldens": 0, "results": []})[1]


@pytest.mark.parametrize("result", [
    {"status": "FAIL", "goldens": 0, "results": [], "error": "denied"},
    {"status": "PASS", "goldens": 1, "results": []},
])
def test_attest_inconsistent(result):
    assert not render(result)[1]


def test_attest_missing_and_invalid(tmp_path):
    result = tmp_path / "missing.json"
    output = tmp_path / "attestation.md"
    command = [sys.executable, str(ROOT / "ci/attest.py"), str(result), "--output", str(output)]
    assert subprocess.run(command).returncode == 1
    assert "**FAIL**" in output.read_text()
    result.write_text("not json")
    assert subprocess.run(command).returncode == 1


def test_classification():
    registry = {"load_sale": {}, "load_customer": {}}
    assert classify({}, ["src/parity/parity_check.py"]) == []
    assert classify(registry, ["src/sql/units/load_sale.sql"]) == ["load_sale"]
    assert classify(registry, ["src/parity/parity_check.py"]) == ["load_customer", "load_sale"]
    assert classify(registry, ["README.md"]) == []


def test_cleanup_scoping():
    schemas = [SimpleNamespace(name=n, comment=c) for n, c in [
        ("gold_pr_1", "daiwt-02-migration"), ("bronze_pr_2", "daiwt-02-migration"),
        ("landing", "daiwt-02-migration"), ("gold_pr_3", "another-repo"),
        ("gold", "daiwt-02-migration"), ("gold_pr_1_extra", "daiwt-02-migration"),
        ("pr_4_silver_pr_4", "daiwt-02-migration")]]
    assert list(candidates(schemas, {1})) == ["gold_pr_1"]
    assert list(candidates(schemas, None)) == ["gold_pr_1", "bronze_pr_2", "pr_4_silver_pr_4"]


def test_zero_units_and_bundle_targets():
    assert json.loads((ROOT / "src/sql/units/registry.json").read_text()) == {}
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "resources" not in bundle["targets"]["prod"]
    assert set(bundle["targets"]["dev"]["resources"]["schemas"]) == {"bronze", "silver", "gold"}
    assert "name_prefix" not in bundle["targets"]["dev"]["presets"]
    assert bundle["targets"]["prod"]["permissions"] == [
        {"service_principal_name": "${workspace.current_user.userName}", "level": "CAN_MANAGE"},
        {"group_name": "users", "level": "CAN_VIEW"}]


def test_workflow_oidc_environments():
    for path in (ROOT / ".github/workflows").glob("*.yml"):
        workflow = yaml.safe_load(path.read_text())
        assert workflow["permissions"]["id-token"] == "write"
        for name, job in workflow["jobs"].items():
            if name == "unit-tests":
                continue
            assert job["environment"] in ("dev", "prod", "${{ inputs.target }}")
            assert job["env"]["DATABRICKS_AUTH_TYPE"] == "github-oidc"
