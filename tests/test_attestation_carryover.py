import json
import subprocess
import sys

import pytest
import databricks.sdk

import check_attestation
from check_attestation import can_carry_over, normalize_permissions
from drops import DROPS
from record_attestation import COLUMNS


@pytest.fixture
def history(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def git(*args, check=True):
        return subprocess.run(["git", *args], check=check, capture_output=True, text=True).stdout.strip()

    def write(path, value):
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(value)

    def commit(message):
        git("add", "src")
        if (tmp_path / "resources").exists():
            git("add", "resources")
        git("-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", message)
        return git("rev-parse", "HEAD")

    registry_path = "src/sql/units/registry.json"
    spec = lambda job: dict(job=job, outputs=[], owns=[], depends_on=[])
    initial = {"own": spec("old"), "other": spec("other"), "removed": spec("removed")}
    git("init", "-q", "-b", "main")
    write(registry_path, json.dumps(initial))
    write("src/parity/contract.json", '{"outputs": []}')
    write("src/sql/units/own/deleted.sql", "old")
    commit("base")
    git("switch", "-c", "pr")
    own = {"own": spec("new"), "other": spec("other"), "added": spec("added")}
    write(registry_path, json.dumps(own))
    write("src/sql/units/own.sql", "SELECT 1")
    (tmp_path / "src/sql/units/own/deleted.sql").unlink()
    candidate = commit("PR changes")
    git("switch", "main")
    main_registry = {**initial, "main_unit": spec("main_job")}
    write(registry_path, json.dumps(main_registry))
    base = commit("main registry change")
    git("switch", "pr")
    git("-c", "user.name=Test", "-c", "user.email=test@example.com", "merge", "--no-commit", "main", check=False)
    merged = {**own, "main_unit": spec("main_job")}
    write(registry_path, json.dumps(merged))
    head = commit("resolve registry merge")
    return dict(git=git, write=write, commit=commit, base=base, candidate=candidate,
                head=head, registry=merged, registry_path=registry_path)


def test_registry_only_main_merge_and_both_deleted_path(history):
    h = history
    assert can_carry_over(h["base"], h["head"], h["candidate"]) == (
        True, "only main-derived registry changes since")


@pytest.mark.parametrize("change,reason", [
    ("unit", "PR blob differs"),
    ("registry", "PR registry entries differ"),
    ("added_path", "PR changed path sets differ"),
])
def test_changed_pr_rejected(history, change, reason):
    h = history
    if change == "unit":
        h["write"]("src/sql/units/own.sql", "SELECT 2")
    elif change == "registry":
        h["registry"]["own"]["job"] = "changed_again"
        h["write"](h["registry_path"], json.dumps(h["registry"]))
    else:
        h["write"]("src/sql/units/another.sql", "SELECT 1")
    head = h["commit"]("more PR changes")
    ok, message = can_carry_over(h["base"], head, h["candidate"])
    assert not ok and reason in message


@pytest.mark.parametrize("fetch_candidate", [False, True])
def test_rebased_non_ancestor_accepted(history, tmp_path, fetch_candidate):
    h = history
    h["git"]("switch", "-c", "rebased", h["base"])
    h["git"]("cherry-pick", "--no-commit", h["candidate"], check=False)
    h["write"](h["registry_path"], json.dumps(h["registry"]))
    head = h["commit"]("rebased PR changes")
    assert subprocess.run(["git", "merge-base", "--is-ancestor", h["candidate"], head]).returncode == 1
    repo = tmp_path
    if fetch_candidate:
        repo = tmp_path / "clone"
        h["git"]("clone", "--no-local", "--single-branch", "-b", "rebased", str(tmp_path), str(repo))
        assert subprocess.run(["git", "cat-file", "-e", h["candidate"]], cwd=repo, capture_output=True).returncode != 0
    assert can_carry_over(h["base"], head, h["candidate"], repo=repo) == (
        True, "only main-derived registry changes since")


@pytest.mark.parametrize("path,is_test", [
    ("tests/assertions.txt", True),
    ("src/sql/units/test_own.py", True),
    ("src/sql/units/own.py", False),
])
@pytest.mark.parametrize("change", ["remove", "add", "change", "delete", "create"])
def test_removal_only_test_carryover(history, path, is_test, change):
    h = history
    original = "def test_own():\n    assert True\n    assert 1 == 1\n    pass\n"
    candidate = h["head"]
    if change != "create":
        h["write"](path, original)
        h["git"]("add", "--", path)
        candidate = h["commit"]("candidate test assertions")
    if change == "delete":
        h["git"]("rm", "--", path)
    else:
        after = original
        if change == "remove":
            after = "def test_own():\n    pass\n"
        elif change == "add":
            after += "    assert 2 == 2\n"
        elif change == "change":
            after = original.replace("assert True", "assert False")
        h["write"](path, after)
        h["git"]("add", "--", path)
    head = h["commit"]("adjust test assertions")
    ok, reason = can_carry_over(h["base"], head, candidate)
    assert ok is (is_test and change == "remove"), reason


def test_unfetchable_candidate_rejected(history):
    candidate = "0" * 40
    assert can_carry_over(history["base"], history["head"], candidate) == (
        False, f"candidate commit {candidate} is missing and could not be fetched from origin")


@pytest.mark.parametrize("extension", ["yml", "yaml"])
@pytest.mark.parametrize("change,accepted", [
    ("users_only", True), ("other_change", False), ("non_users", False),
    ("users_remain", False), ("invalid_yaml", False),
])
def test_resource_permissions_carryover(history, extension, change, accepted):
    h = history
    path = f"resources/own.{extension}"
    common = "resources:\n  jobs:\n    own:\n      name: own\n      tasks: []\n"
    overrides = ("targets:\n  dev:\n    resources:\n      jobs:\n        own:\n          permissions:\n"
                 "            - group_name: users\n              level: CAN_VIEW\n"
                 "      pipelines:\n        own_pipeline:\n          permissions:\n"
                 "            - group_name: users\n              level: CAN_VIEW\n")
    if change == "non_users":
        overrides = overrides.replace("group_name: users", "group_name: operators")
    h["write"](path, common + overrides)
    candidate = h["commit"]("resource with grants")
    after = common
    if change == "other_change":
        after = common.replace("name: own", "name: changed")
    elif change == "users_remain":
        after = common + overrides.replace("CAN_VIEW", "CAN_MANAGE")
    elif change == "invalid_yaml":
        after = "resources: [invalid"
    h["write"](path, after)
    head = h["commit"]("remove grants")
    ok, reason = can_carry_over(h["base"], head, candidate)
    assert ok is accepted, reason


def test_permission_normalization_preserves_original_empty_containers():
    original = {"empty_dict": {}, "empty_list": [], "permissions": []}
    assert normalize_permissions(original) == (original, False)
    value = {**original, "targets": {"dev": {"permissions": [{"group_name": "users"}]}}}
    assert normalize_permissions(value) == (original, True)


@pytest.mark.parametrize("candidate", ["missing", "0" * 40, None])
def test_missing_candidate_fails_closed(history, candidate):
    assert not can_carry_over(history["base"], history["head"], candidate)[0]


@pytest.mark.parametrize("exact,failure", [
    (True, None), (False, None), (False, "units"), (False, "drops"),
    (False, "attestation_md"), (True, "units"),
])
def test_exact_head_and_carryover_paths(history, monkeypatch, tmp_path, exact, failure):
    h = history
    row = dict(head_sha=h["head"] if exact else h["candidate"], suffix="pr_1", status="PASS",
               units=json.dumps(["own", "added"]), selection="changed",
               drops=json.dumps({drop: {"status": "PASS"} for drop in DROPS}),
               attestation_md="# original evidence", run_urls="[]", recorded_by="Devin", recorded_at="now")
    if failure:
        row[failure] = {"units": "[]", "drops": "{}", "attestation_md": ""}[failure]
    queries = []

    class Warehouse:
        def __init__(self, *args):
            pass

        def sql(self, query, parameters):
            queries.append(query)
            if "head_sha = :head" in query and not exact:
                return []
            if not exact and not failure:
                missing = dict(row, head_sha="0" * 40)
                return [[entry[column] for column in COLUMNS] for entry in (missing, row)]
            return [[row[column] for column in COLUMNS]]

    monkeypatch.setattr(check_attestation, "Warehouse", Warehouse)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda: None)
    monkeypatch.setattr(sys, "argv", ["check_attestation", "--head-sha", h["head"],
                                    "--base-sha", h["base"], "--suffix", "pr_1"])
    if exact:
        monkeypatch.setattr(check_attestation, "can_carry_over", lambda *args: pytest.fail("unexpected carry-over"))
    assert check_attestation.main() == (1 if failure else 0)
    body = (tmp_path / "attestation.md").read_text()
    if failure:
        assert "**FAIL**" in body
        assert "carried over" not in body
        return
    assert "# original evidence" in body
    assert len(queries) == (1 if exact else 2)
    if exact:
        assert "carried over" not in body
    else:
        assert f"carried over from {h['candidate']} (only main-derived registry changes since)" in body
