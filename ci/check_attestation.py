"""Require current pushed-head evidence before merging."""
import argparse
import json
import re
import subprocess
from pathlib import Path

import yaml

from classify_changes import classify_diff
from drops import DROPS
from registry import validate
from record_attestation import COLUMNS, TABLE, Warehouse, table_name


def evaluate(rows, head_sha, suffix, required_units):
    if not rows:
        return False, "no attestation for this head and suffix"
    row = rows[0]
    if row.get("head_sha") != head_sha or row.get("suffix") != suffix:
        return False, "head or suffix mismatch"
    if row.get("status") != "PASS":
        return False, "latest attestation is not PASS"
    try:
        drops = json.loads(row["drops"])
        units = json.loads(row["units"])
        if not isinstance(drops, dict) or set(drops) != set(DROPS) or any(drops[d]["status"] != "PASS" for d in DROPS):
            return False, "missing or failed replay drop"
        if not isinstance(units, list) or not all(isinstance(unit, str) for unit in units):
            return False, "invalid recorded units"
        if not set(required_units) <= set(units):
            return False, "insufficient recorded units"
        if not isinstance(row["attestation_md"], str) or not row["attestation_md"]:
            return False, "missing attestation markdown"
    except (ValueError, KeyError, TypeError):
        return False, "invalid attestation data"
    return True, "PASS"


def normalize_permissions(value):
    pruned = object()

    def visit(node, permissions=False):
        removed = False
        if isinstance(node, dict):
            result = {}
            for key, value in node.items():
                child, changed = visit(value, key == "permissions")
                removed |= changed
                if child is not pruned:
                    result[key] = child
        elif isinstance(node, list):
            result = []
            for value in node:
                if permissions and isinstance(value, dict) and value.get("group_name") == "users":
                    removed = True
                    continue
                child, changed = visit(value)
                removed |= changed
                if child is not pruned:
                    result.append(child)
        else:
            return node, False
        return (pruned if node and not result and removed else result), removed

    normalized, removed = visit(value)
    return (None if normalized is pruned else normalized), removed


def can_carry_over(base_sha, head_sha, candidate_sha, repo=None):
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=repo, text=True, stderr=subprocess.PIPE)

    def paths(base, head):
        return set(filter(None, git("diff", "--name-only", "-z", base, head).split("\0")))

    def blob(head, path):
        if not git("ls-tree", "-z", head, "--", path):
            return None
        return git("rev-parse", f"{head}:{path}").strip()

    def own_entries(base, head):
        path = "src/sql/units/registry.json"
        before = json.loads(git("show", f"{base}:{path}"))
        after = json.loads(git("show", f"{head}:{path}"))
        if not isinstance(before, dict) or not isinstance(after, dict):
            raise ValueError("registry must be an object")
        return {unit: (unit in after, after.get(unit)) for unit in before.keys() | after.keys()
                if (unit in before, before.get(unit)) != (unit in after, after.get(unit))}

    try:
        if not isinstance(candidate_sha, str) or not re.fullmatch(r"[0-9a-f]{40,64}", candidate_sha):
            return False, "invalid candidate head"
        try:
            git("rev-parse", "--verify", f"{candidate_sha}^{{commit}}")
        except subprocess.CalledProcessError:
            try:
                git("fetch", "--no-tags", "origin", candidate_sha)
                git("rev-parse", "--verify", f"{candidate_sha}^{{commit}}")
            except subprocess.CalledProcessError:
                return False, f"candidate commit {candidate_sha} is missing and could not be fetched from origin"
        base = git("merge-base", base_sha, head_sha).strip()
        candidate_base = git("merge-base", base_sha, candidate_sha).strip()
        own_paths = paths(base, head_sha)
        if own_paths != paths(candidate_base, candidate_sha):
            return False, "PR changed path sets differ"
        for path in sorted(own_paths - {"src/sql/units/registry.json"}):
            if blob(candidate_sha, path) != blob(head_sha, path):
                if re.fullmatch(r"resources/[^/]+\.ya?ml", path):
                    before, _ = normalize_permissions(yaml.safe_load(git("show", f"{candidate_sha}:{path}")))
                    after, has_users = normalize_permissions(yaml.safe_load(git("show", f"{head_sha}:{path}")))
                    if before == after and not has_users:
                        continue
                return False, f"PR blob differs: {path}"
        if own_entries(base, head_sha) != own_entries(candidate_base, candidate_sha):
            return False, "PR registry entries differ"
    except (subprocess.CalledProcessError, OSError, ValueError, yaml.YAMLError, RecursionError) as exc:
        return False, f"carry-over comparison failed: {exc}"
    return True, "only main-derived registry changes since"


def main():
    from databricks.sdk import WorkspaceClient
    parser = argparse.ArgumentParser()
    for name in ("head-sha", "base-sha", "suffix"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--table", default=TABLE)
    parser.add_argument("--output", default="attestation.md")
    args = parser.parse_args()
    try:
        registry = json.loads(subprocess.check_output(["git", "show", f"{args.head_sha}:src/sql/units/registry.json"], text=True))
        contract = json.loads(subprocess.check_output(["git", "show", f"{args.head_sha}:src/parity/contract.json"], text=True))
        validate(registry, contract["outputs"])
        base = subprocess.check_output(["git", "merge-base", args.base_sha, args.head_sha], text=True).strip()
        required_units = classify_diff(registry, base, args.head_sha)
        warehouse = Warehouse(WorkspaceClient(), "6dc4b80538621b7b")
        data = warehouse.sql(f"SELECT {', '.join(COLUMNS)} FROM {table_name(args.table)} WHERE head_sha = :head AND suffix = :suffix ORDER BY recorded_at DESC",
                             {"head": args.head_sha, "suffix": args.suffix})
        rows = [dict(zip(COLUMNS, row)) for row in data]
        ok, reason = evaluate(rows, args.head_sha, args.suffix, required_units)
        carried_from = None
        if not ok and not any(row.get("status") == "PASS" for row in rows):
            data = warehouse.sql(f"SELECT {', '.join(COLUMNS)} FROM {table_name(args.table)} WHERE suffix = :suffix AND status = 'PASS' ORDER BY recorded_at DESC",
                                 {"suffix": args.suffix})
            candidates = [dict(zip(COLUMNS, row)) for row in data]
            for candidate in candidates:
                candidate_sha = candidate.get("head_sha")
                matches, reason = can_carry_over(args.base_sha, args.head_sha, candidate_sha)
                if not matches:
                    continue
                ok, reason = evaluate([candidate], candidate_sha, args.suffix, required_units)
                if ok:
                    rows = [candidate]
                    carried_from = candidate_sha
                    break
    except Exception as exc:
        ok, reason = False, f"attestation query or selection failed: {exc}"
    body = f"# DAIWT-02 attestation\n\n**FAIL** — {reason}\n"
    if ok:
        row = rows[0]
        body = f"Head {args.head_sha} — {row['recorded_by']} — {row['recorded_at']}\n\n" + row["attestation_md"]
        if carried_from:
            body = f"Head {args.head_sha} — carried over from {carried_from} (only main-derived registry changes since)\n\n" + row["attestation_md"]
    Path(args.output).write_text(body)
    print(reason)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
