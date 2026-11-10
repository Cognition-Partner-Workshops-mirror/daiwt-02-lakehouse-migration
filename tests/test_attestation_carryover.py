import json
import subprocess
import sys

import pytest
import databricks.sdk

import check_attestation
from check_attestation import can_carry_over
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


def test_non_ancestor_rejected(history):
    h = history
    h["git"]("switch", "-c", "unrelated", h["base"])
    h["write"]("src/unrelated", "unrelated")
    other = h["commit"]("not in PR history")
    assert can_carry_over(h["base"], h["head"], other) == (
        False, "candidate head is not an ancestor of current head")


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
