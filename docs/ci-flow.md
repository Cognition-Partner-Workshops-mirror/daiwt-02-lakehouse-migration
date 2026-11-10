# Per-drop CI and builder flow

## Register and select units

[`registry.json`](../src/sql/units/registry.json) entries require exactly `job`,
`outputs`, `owns`, `depends_on`. Jobs are unique non-empty bundle job keys;
`outputs` contains contract tables, `owns` other cleanup-owned tables, and
`depends_on` existing upstream units. Tables have one owner, use unsuffixed
`bronze|silver|gold.<name>` identifiers, and dependencies must be acyclic.
Do not re-list bronze inputs in Silver. See [registry rules](registry.md).
Jobs accept `drop`, `last_cutoff`, `new_cutoff` parameters.

[`classify_changes.py`](../ci/classify_changes.py) is shared by verification
and the evidence gate:

- A unit's `src/sql/units/<unit>.sql`, unit directory or `resources/<unit>.job.yml`
  selects that unit plus transitive downstream dependants.
- Registry changes select added/changed entries (JSON equality against base) plus
  transitive upstream `depends_on` units. Removed entries select nothing.
- `tests/`, paths starting with `README`, `docs/`, `.github/` are ignored.
  Other unmatched/shared/framework paths select all. Differently named resources
  are not automatically unit-scoped.
- Explicit comma/space lists expand downstream dependants; `'*'` selects all.
  Do not assume explicit lists add missing upstreams.

Execution is dependencies-first among selected units, registry order breaking ties.
Omitted/empty units classify against the merge base with `origin/main`.

## Verify a pushed head in dev

After opening a PR and pushing, activate the requirements-dev venv:

```bash
databricks current-user me
bash ci/dev_verify.sh pr_123 ''  # classifier; '*' selects all
```

[`dev_verify.sh`](../ci/dev_verify.sh) requires a clean worktree and HEAD on a
remote branch. It fetches origin, validates/deploys **dev only**, cleans selected
registered objects, and writes `out/selected-units.json`. Selected jobs execute
in dependency order with parity after each drop, **D0 -> D1 -> D2**, passing
`--units` to parity. Pipeline-owned objects skipped by cleanup need D0 full refresh
in their unit. See [drop replay](drop-replay.md) for restart rules.

Outputs: `out/parity-D0.json`, `out/parity-D1.json`, `out/parity-D2.json`,
`out/run-urls.txt`, `attestation.md`. The script stops replay at first failure,
still attempts Markdown/evidence recording, and records FAIL for missing/invalid
drops. `daiwt_02_mig_dev.default.pr_attestation` records head SHA, suffix, status,
units, selection, drop results, Markdown, run URLs, identity and timestamp.
A new code push normally needs fresh verification; only the checked exceptions
below can reuse earlier evidence.

## Exact-head evidence and carry-over

[`check_attestation.py`](../ci/check_attestation.py) validates the registry and
computes the same selection. It first evaluates the newest `(head, suffix)` row:
PASS, exactly D0/D1/D2 all PASS, a valid unit list covering the selection, and
non-empty Markdown. A PASS status alone does not bypass these checks.

Only if there is **no exact-head PASS row** does it try suffix PASS rows newest-first.
For candidate A and current head H:

1. A must exist locally; missing commits are fetched with
   `git fetch --no-tags origin <A>`. Failed fetch rejects. A need not be an ancestor
   (rebases work).
2. Own changed paths against each head's merge base with the supplied base SHA must
   match. PR-added/changed/removed registry entries must match names and resulting
   specs; unrelated main-derived entries may differ.
3. Other own blobs must be byte-identical, except:
   - `resources/*.yml` / `*.yaml`: safe-loaded YAML must match after recursively
     removing `group_name: users` mappings from permissions lists and pruning
     containers emptied by removal. Originally empty containers remain. H must
     have no such grants; YAML parse errors reject.
   - Files under `tests/` or with basename `test_*.py`: both versions must exist;
     `git diff --unified=0 A H -- <path>` hunks must contain at least one removed
     content line and no added content lines. Creation/deletion rejects.
4. The candidate must still pass evidence validation for the current selection.

The sticky comment attributes carry-over to A and includes its original Markdown.
Git, parsing, selection and evidence errors remain fail-closed.

## Workflows and permissions

| Workflow | Trigger and scope |
| --- | --- |
| [`pr.yml`](../.github/workflows/pr.yml) | PRs to main/pre-migration: offline pytest; same-repo PRs also strict dev validation and UC evidence/sticky comment. No deployment or job execution. |
| [`deploy-prod.yml`](../.github/workflows/deploy-prod.yml) | Main push or manual: strict prod validation, deploy, all registered jobs and parity for D0/D1/D2 (no units filter), Markdown/artifacts. Missing/failed parity or attestation fails the workflow. |
| [`dev-sweep.yml`](../.github/workflows/dev-sweep.yml) | Manual: closed-PR cleanup, or explicitly requested `suffix=pr_<n>`. |

Tests run on PRs, not as a separate main-push workflow. Both bundle targets grant
`users CAN_VIEW` at target level; **never duplicate users grants on jobs/pipelines**,
including target resource overrides. Pytest guards this collision. Local strict
validation checks both targets without deploying production.

If attestation ran before evidence existed, rerun failed jobs **after** PASS is
recorded via `POST /repos/<owner>/<repo>/actions/runs/<id>/rerun-failed-jobs`.
Investigate other failures; do not rerun prod deployment as an attestation fix.

## Teardown

After merge/closure and explicit teardown approval, use the original deployment
principal (bundle state is under its workspace root):

```bash
databricks bundle destroy -t dev --var schema_suffix=pr_123 --auto-approve
```

Check `daiwt_02_mig_dev` for remaining suffix schemas. For CI-owned leftovers,
dispatch dev-sweep with that suffix; do not force ownership. Schema deletion
requires the bundle marker and PR naming pattern; selected-unit cleanup instead
drops registered `outputs` and `owns`. Never target `landing`, goldens or
Terraform-owned production schemas. See [governance](governance.md) for access
boundaries and [docs/daily-etl.md (P8)](daily-etl.md) for the separately owned DAG.
