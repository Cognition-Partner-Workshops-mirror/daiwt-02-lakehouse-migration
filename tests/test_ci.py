import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from attest import render
from classify_changes import classify, classify_diff, expand
from check_attestation import evaluate
from drops import DROPS, drop_params
from cleanup import candidates
from run_units import run_units

ROOT = Path(__file__).resolve().parents[1]


def mock_runs(monkeypatch, calls, code=0):
    class Process:
        stdout = ["Run URL: https://example/run/1\n", "completed\n"]
        returncode = code

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def wait(self):
            return self.returncode
    def popen(command, **kwargs):
        calls.append(command)
        return Process()
    monkeypatch.setattr(subprocess, "Popen", popen)


def test_dev_runs_all_registered_units(monkeypatch):
    calls = []
    mock_runs(monkeypatch, calls)
    urls = run_units("pr_1", {"sale": {"job": "sale_job"}, "customer": {"job": "customer_job"}}, drop="D1")
    assert [command[-1] for command in calls] == ["sale_job", "customer_job"]
    assert all(command[4:6] == ["dev", "--var"] for command in calls)
    script = (ROOT / "ci/dev_verify.sh").read_text()
    assert 'out/selected-units.json --drop "$D" --run-urls out/run-urls.txt' in script
    assert urls == ["Run URL: https://example/run/1"] * 2
    assert all(command[7:9] == ["--params", "drop=D1,last_cutoff=2016-06-06 12:31:17.0000000,new_cutoff=2016-06-14 00:00:00.0000000"] for command in calls)


@pytest.mark.parametrize("target,suffix", [("dev", "pr_1"), ("prod", "prod")])
def test_run_units_empty(monkeypatch, target, suffix):
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: pytest.fail("unexpected job"))
    assert run_units(suffix, {}, target=target, drop="D0") == []


def test_prod_runs_registered_units_before_parity(monkeypatch):
    calls = []
    mock_runs(monkeypatch, calls)
    run_units("prod", {"sale": {"job": "sale_job"}, "customer": {"job": "customer_job"}}, target="prod", drop="D2")
    assert [command[-1] for command in calls] == ["sale_job", "customer_job"]
    assert all(command[4:7] == ["prod", "--var", "schema_suffix=prod"] for command in calls)
    workflow = yaml.safe_load((ROOT / ".github/workflows/deploy-prod.yml").read_text())
    commands = "\n".join(step.get("run", "") for step in workflow["jobs"]["deploy"]["steps"])
    positions = [commands.index("databricks bundle deploy -t prod")]
    for drop in DROPS:
        positions.extend([commands.index(f"python ci/run_units.py prod --target prod --drop {drop}"),
                          commands.index(f"python src/parity/parity_check.py --catalog daiwt_02_mig_prod --suffix prod --drop {drop}")])
    assert positions == sorted(positions)


@pytest.mark.parametrize("target,suffix", [("dev", "prod"), ("prod", "pr_1"), ("other", "pr_1")])
def test_run_units_rejects_invalid_target_suffix(target, suffix):
    with pytest.raises(ValueError):
        run_units(suffix, {}, target=target)


def test_run_units_failure(monkeypatch):
    mock_runs(monkeypatch, [], code=1)
    with pytest.raises(subprocess.CalledProcessError):
        run_units("pr_1", {"sale": {"job": "sale_job"}}, drop="D0")


@pytest.mark.parametrize("status,ok", [("PASS", True), ("SKIP", True), ("FAIL", False), ("UNKNOWN", False)])
def test_attestation(status, ok):
    result = parity_result()
    result["results"][0].update(status=status, reason="detail")
    result["counts"] = {s: sum(r["status"] == s for r in result["results"])
                        for s in ("PASS", "FAIL", "SKIP")}
    result["compared"] = result["counts"]["PASS"] + result["counts"]["FAIL"]
    text, valid = render(result)
    assert valid is ok
    assert f"| gold.table_0 | D2 | {status} | detail |" in text


def test_attest_empty():
    text, valid = render(parity_result())
    assert valid
    assert "0 outputs compared — not parity evidence" in text
    assert "compared 0/18" in text


def parity_result(drop="D2"):
    return {"status": "PASS", "drop": drop, "goldens": 18,
            "counts": {"PASS": 0, "FAIL": 0, "SKIP": 18}, "compared": 0,
            "results": [{"table": f"gold.table_{i}", "drop": drop,
                         "status": "SKIP", "reason": "not yet migrated"} for i in range(18)]}


@pytest.mark.parametrize("result", [
    {**parity_result(), "status": "FAIL", "error": "denied"},
    {**parity_result(), "goldens": 1},
    {**parity_result(), "compared": 1},
    {**parity_result(), "counts": {"PASS": 1, "FAIL": 0, "SKIP": 17}},
    {**parity_result(), "results": [parity_result()["results"][0]] * 18},
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


def test_attest_multiple_files(tmp_path):
    paths = []
    for drop in ("D0", "D1", "D2"):
        path = tmp_path / f"{drop}.json"
        path.write_text(json.dumps(parity_result(drop)))
        paths.append(str(path))
    output = tmp_path / "attestation.md"
    command = [sys.executable, str(ROOT / "ci/attest.py"), *paths, "--output", str(output)]
    assert subprocess.run(command).returncode == 0
    assert all(f"PASS {drop}" in output.read_text() for drop in ("D0", "D1", "D2"))
    Path(paths[1]).write_text("{}")
    assert subprocess.run(command).returncode == 1
    assert "**FAIL**" in output.read_text()


def test_classification():
    registry = {"load_sale": {}, "load_customer": {}}
    assert classify({}, ["src/parity/parity_check.py"]) == []
    assert classify(registry, ["src/sql/units/load_sale.sql"]) == ["load_sale"]
    assert classify(registry, ["src/parity/parity_check.py"]) == ["load_sale", "load_customer"]
    assert classify(registry, ["README.md"]) == []


def test_cleanup_scoping():
    schemas = [SimpleNamespace(name=n, comment=c) for n, c in [
        ("gold_pr_1", "daiwt-02-migration"), ("bronze_pr_2", "daiwt-02-migration"),
        ("landing", "daiwt-02-migration"), ("gold_pr_3", "another-repo"),
        ("gold", "daiwt-02-migration"), ("gold_pr_1_extra", "daiwt-02-migration"),
        ("pr_4_silver_pr_4", "daiwt-02-migration"), ("pr_1_gold_pr_4", "daiwt-02-migration")]]
    assert list(candidates(schemas, {1})) == ["gold_pr_1"]
    assert list(candidates(schemas, None)) == ["gold_pr_1", "bronze_pr_2", "pr_4_silver_pr_4"]


def test_registry_and_bundle_targets():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert all(set(spec) >= {"job", "outputs"} for spec in registry.values())
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "resources" not in bundle["targets"]["prod"]
    assert set(bundle["targets"]["dev"]["resources"]["schemas"]) == {"bronze", "silver", "gold"}
    assert "name_prefix" not in bundle["targets"]["dev"]["presets"]
    assert bundle["targets"]["prod"]["permissions"] == [
        {"service_principal_name": "${workspace.current_user.userName}", "level": "CAN_MANAGE"},
        {"group_name": "users", "level": "CAN_VIEW"}]


@pytest.mark.parametrize("target", ["dev", "prod"])
def test_bundle_permissions_do_not_duplicate_target_principals(target):
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    permissions = bundle["targets"][target]["permissions"]
    assert {"group_name": "users", "level": "CAN_VIEW"} in permissions
    principal_keys = ("group_name", "user_name", "service_principal_name")
    principals = {(key, permission[key]) for permission in permissions
                  for key in principal_keys if key in permission}

    def check(resources, path):
        for kind, entries in resources.items():
            for name, resource in entries.items():
                for permission in resource.get("permissions", []):
                    assert permission.get("group_name") != "users", f"{path}: {kind}.{name} grants users"
                    assert not principals.intersection(
                        (key, permission[key]) for key in principal_keys if key in permission
                    ), f"{path}: {kind}.{name} duplicates {target} permissions"

    documents = [(ROOT / "databricks.yml", bundle)]
    for pattern in bundle["include"]:
        documents.extend((path, yaml.safe_load(path.read_text())) for path in ROOT.glob(pattern))
    for path, document in documents:
        check(document.get("resources", {}), path)
        check(document.get("targets", {}).get(target, {}).get("resources", {}), path)


def test_workflow_oidc_environments():
    for path in (ROOT / ".github/workflows").glob("*.yml"):
        workflow = yaml.safe_load(path.read_text())
        assert workflow["permissions"]["id-token"] == "write"
        for name, job in workflow["jobs"].items():
            if name == "unit-tests":
                continue
            assert job["environment"] in ("dev", "prod", "${{ inputs.target }}")
            assert job["env"]["DATABRICKS_AUTH_TYPE"] == "github-oidc"


@pytest.mark.parametrize("path,expected", [
    ("src/sql/units/a.sql", ["a", "b", "c"]),
    ("src/sql/units/a/query.sql", ["a", "b", "c"]),
    ("resources/a.job.yml", ["a", "b", "c"]),
    ("ci/drops.py", ["a", "b", "c", "z"]),
    ("docs/readme.md", []), ("tests/test_ci.py", []), ("README.md", []), (".github/workflows/pr.yml", [])])
def test_selection(path, expected):
    registry = {"c": {"depends_on": ["b"]}, "a": {}, "b": {"depends_on": ["a"]}, "z": {}}
    assert classify(registry, [path]) == expected


def test_expand():
    registry = {"b": {"depends_on": ["a"]}, "a": {}, "c": {}}
    assert expand(registry, ["a"]) == ["a", "b"]
    assert expand(registry, "*") == ["a", "b", "c"]
    assert expand(registry, []) == []
    with pytest.raises(ValueError):
        expand(registry, ["unknown"])


@pytest.mark.parametrize("change,expected", [
    ("changed", ["a", "b", "c"]),
    ("added", ["a", "b", "c"]),
    ("unchanged", []),
    ("removed", []),
])
def test_registry_entry_selection(change, expected):
    registry = {"c": {"depends_on": ["b"]}, "a": {},
                "b": {"depends_on": ["a"]}, "downstream": {"depends_on": ["c"]}, "z": {}}
    base = json.loads(json.dumps(registry))
    if change == "changed":
        base["c"]["job"] = "old_job"
    elif change == "added":
        del base["c"]
    elif change == "removed":
        base["removed"] = {}
    assert classify(registry, ["src/sql/units/registry.json"], base) == expected


def test_registry_selection_mixed_paths():
    base = {"a": {}, "b": {"depends_on": ["a"]}, "c": {"depends_on": ["b"]}, "z": {}}
    registry = dict(base, b={"depends_on": ["a"], "job": "new"})
    assert classify(registry, ["src/sql/units/registry.json", "src/sql/units/c.sql"], base) == ["a", "b", "c"]
    assert classify(registry, ["src/sql/units/registry.json", "ci/drops.py"], base) == ["a", "b", "c", "z"]
    with pytest.raises(ValueError, match="base registry required"):
        classify(registry, ["src/sql/units/registry.json"])


def test_registry_diff_simulation(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    subprocess.run(["git", "init", "-q"], check=True)
    registry_path = tmp_path / "src/sql/units/registry.json"
    registry_path.parent.mkdir(parents=True)
    contract_path = tmp_path / "src/parity/contract.json"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text((ROOT / "src/parity/contract.json").read_text())
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    registry_path.write_text(json.dumps(registry))
    subprocess.run(["git", "add", "src"], check=True)
    commit = ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm"]
    subprocess.run(commit + ["base"], check=True)
    base = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    registry["new_entry"] = {"job": "new_job", "outputs": [], "owns": [],
                             "depends_on": ["bronze_landing_replay"]}
    registry_path.write_text(json.dumps(registry))
    subprocess.run(["git", "add", "src/sql/units/registry.json"], check=True)
    subprocess.run(commit + ["add entry"], check=True)
    expected = ["bronze_landing_replay", "new_entry"]
    assert classify_diff(registry, base, "HEAD") == expected
    result = subprocess.check_output([sys.executable, str(ROOT / "ci/classify_changes.py"), base, "HEAD"], text=True)
    assert json.loads(result) == expected
    print("Simulated registry-only addition: " + result.strip())


def test_real_drop_params():
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    values = ["2016-06-06 12:31:17.0000000", "2016-06-14 00:00:00.0000000", "2016-06-21 00:00:00.0000000"]
    for index, drop in enumerate(DROPS):
        assert drop_params(manifest, drop) == {"drop": drop, "last_cutoff": values[max(0, index - 1)], "new_cutoff": values[index]}
    with pytest.raises(ValueError):
        drop_params(manifest, "D3")


@pytest.mark.parametrize("value", ["2016-06-14", "2016-06-14T00:00:00", "2016-02-30 00:00:00", "2016-06-14 00:00:00.12345678", [],
    [{"Table Name": "City", "Cutoff Time": "2016-06-14 00:00:00"}, {"Table Name": "Sale", "Cutoff Time": "2016-06-15 00:00:00"}]])
def test_bad_cutoffs(value):
    with pytest.raises(ValueError):
        drop_params({"drops": [{"name": "D0", "last_cutoff": value, "new_cutoff": value}]}, "D0")


def evidence(**values):
    return {"head_sha": "head", "suffix": "pr_23", "status": "PASS", "units": '["a", "b"]',
            "drops": json.dumps({drop: {"status": "PASS", "counts": {}} for drop in DROPS}),
            "attestation_md": "# evidence", **values}


@pytest.mark.parametrize("rows,required,ok", [
    ([], [], False), ([evidence(status="FAIL"), evidence()], [], False),
    ([evidence(drops='{"D0":{"status":"PASS"}}')], [], False),
    ([evidence(drops=json.dumps({d: {"status": "FAIL" if d == "D1" else "PASS"} for d in DROPS}))], [], False),
    ([evidence(units="[]")], ["a"], False), ([evidence(head_sha="old")], [], False),
    ([evidence(suffix="pr_24")], [], False), ([evidence(drops="bad")], [], False),
    ([evidence(units="null")], [], False), ([evidence()], ["a"], True)])
def test_evaluate(rows, required, ok):
    assert evaluate(rows, "head", "pr_23", required)[0] is ok


def test_pr_only_reads_evidence():
    workflow = (ROOT / ".github/workflows/pr.yml").read_text()
    assert all(command not in workflow for command in ("bundle deploy", "run_units", "dev_verify"))


def test_run_urls_survive_failure(monkeypatch, tmp_path):
    mock_runs(monkeypatch, [], code=1)
    path = tmp_path / "urls.txt"
    with pytest.raises(subprocess.CalledProcessError):
        run_units("pr_1", {"a": {"job": "a"}}, drop="D0", run_urls=path)
    assert path.read_text() == "Run URL: https://example/run/1\n"


def test_cleanup_single_suffix():
    schemas = [SimpleNamespace(name=f"gold_pr_{n}", comment="daiwt-02-migration") for n in (1, 2)]
    assert list(candidates(schemas, None, "pr_2")) == ["gold_pr_2"]
