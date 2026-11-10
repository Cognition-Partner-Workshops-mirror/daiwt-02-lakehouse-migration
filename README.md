# DAIWT-02 Lakehouse Migration

SQL-unit migration scaffold for the Wide World Importers estate:
SQL Server `WideWorldImporters`, `WideWorldImportersDW` and SSIS packages.

## Layout

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Dev and production bundle targets |
| `src/sql/units/` | Migrated SQL files and `registry.json`; currently zero units |
| `src/parity/parity_check.py` | Golden CSV multiset comparison using SDK Files and SQL Statements APIs |
| `ci/dev_verify.sh` | Deploy dev, replay selected jobs and parity for D0/D1/D2, record UC evidence |
| `ci/attest.py` | Fail-closed Markdown attestation from parity JSON |
| `ci/reset.sh`, `ci/sweep_dev.sh` | Scoped cleanup |
| `tests/` | Offline pytest tests |

## Targets and authentication

- Dev: `daiwt_02_mig_dev`, schemas `bronze_pr_<n>`, `silver_pr_<n>`, `gold_pr_<n>`.
  The bundle owns these schemas and uses a separate workspace root per suffix.
- Prod: `daiwt_02_mig_prod`, fixed `bronze`, `silver`, `gold`; these and `landing`
  belong to Terraform, not this bundle. Production resources grant the deploying
  service principal CAN_MANAGE and workspace `users` CAN_VIEW.
- SQL warehouse: `6dc4b80538621b7b` (Serverless Starter Warehouse).
- Golden volume: `/Volumes/daiwt_02_mig_prod/landing/golden`.

Locally use the default Devin Databricks CLI profile and unified SDK authentication.
GitHub Actions uses Devin-CI with `DATABRICKS_AUTH_TYPE=github-oidc`, repository
variables `DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID`, and `id-token: write`.
Every Databricks job sets environment `dev` or `prod` to match federation policy.
No GitHub secrets are required.

## Golden contract

The volume contains `inputs/`, `outputs/` and `manifest.json`. The manifest format is:

```json
{
  "tables": [
    {
      "table": "gold.fact_sale",
      "path": "outputs/fact_sale.csv",
      "columns": ["sale_key", "amount"]
    }
  ]
}
```

Each output is a UTF-8 CSV with a header matching the ordered `columns` array.
Use SQL Statements API string representations for values (including decimal scale
and timestamps); `\N` represents SQL NULL. Empty strings and NULL are distinct.
Tables are identified by `<bronze|silver|gold>.<table>`; dev suffixes are resolved by
the harness. No sampling, tolerance, deduplication or ignored columns is applied.
Rows compare as multisets using row count and SHA-256 of sorted canonical JSON rows.

An existing volume with no manifest, or a manifest with `{"tables": []}`, reports
`PASS: 0 goldens`. A missing volume, denied access, malformed manifest, unreadable
migrated golden or failed SQL query fails closed. Each unregistered output reports
SKIP (`not yet migrated`) without reading its CSV or querying a target table.
Registered outputs report PASS only for exact row-count and content matches.

## Adding a unit

1. Add `src/sql/units/<unit>.sql` and its bundle job resource, included from
   `databricks.yml`. Pass catalog and layer schema variables to the job, and name it
   `${var.job_name_prefix}<unit>` (dev `[pr_<n>] <unit>`, prod `<unit>`).
2. Register the bundle job key and all produced golden tables:

   ```json
   {"load_sale": {"job": "load_sale", "outputs": ["gold.fact_sale"], "depends_on": ["load_customer"]}}
   ```

3. Add offline tests. Each output has exactly one producer. Registry insertion order
   determines job execution order; list upstream units before their consumers.

Unit jobs must declare job-level replay parameters:

```yaml
resources:
  jobs:
    load_sale:
      name: ${var.job_name_prefix}load_sale
      parameters:
        - name: drop
          default: ${var.drop}
        - name: last_cutoff
          default: ${var.last_cutoff}
        - name: new_cutoff
          default: ${var.new_cutoff}
      tasks: []
```

`depends_on` is optional and lists upstream registry keys. SQL files, unit directories,
and `resources/<unit>.job.yml` select their unit plus transitive dependants in registry
order. Registry and shared changes select all; README, docs, workflows and tests select
none. Omitted units in `dev_verify` classify against the merge base of `origin/main`;
`'*'` selects all, a comma/space list selects explicit units plus dependants.

## CI

| Workflow | Trigger and behavior |
| --- | --- |
| `pr.yml` | PRs to `main` or `pre-migration`; offline tests, strict dev validation, pushed-head UC evidence check and sticky attestation; no deployment or job execution |
| `deploy-prod.yml` | Push to `main` or manual; strict prod validation, deployment, D0 -> D1 -> D2 jobs and parity, three-drop attestation artifacts |
| `reset.yml` | Manual `target=dev|prod`; runs only if `confirm` exactly matches target |
| `dev-sweep.yml` | Manual; removes closed PR deployments, or a single optional `suffix=pr_<n>` deployment |

Reset dev removes only `bronze|silver|gold_pr_<n>` schemas bearing this bundle's
comment marker and destroys their suffix-scoped bundle jobs. Sweep uses the same
scope but only for closed PR numbers obtained from GitHub. Production reset destroys
this bundle's production jobs and drops only tables listed in the units registry;
it leaves Terraform-owned schemas in place. `landing` and its golden volume are
never cleanup targets. Bundle destruction must use the original deployment principal
so state resolves to the same workspace user root.
There are currently no production tables or jobs to reset.

## Local checks

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -q
databricks bundle validate --strict -t prod
databricks bundle validate --strict -t dev --var schema_suffix=pr_1
python ci/drops.py D1
bash ci/dev_verify.sh pr_<n> '*'
python ci/attest.py out/parity-D0.json out/parity-D1.json out/parity-D2.json
```

Parity writes `out/parity-D0.json`, `out/parity-D1.json`, `out/parity-D2.json`;
attestation writes `attestation.md`. Replay stops on the first failed drop and records
FAIL evidence even when files are missing. The worktree must be clean and HEAD pushed.

## Builder flow

Open PR -> push -> run `bash ci/dev_verify.sh pr_<n> '<units>'` as Devin at the exact
pushed head -> evidence row in `daiwt_02_mig_dev.default.pr_attestation` -> rerun the
failed `attestation` check with REST
`POST /repos/<o>/<r>/actions/runs/<id>/rerun-failed-jobs` -> merge ->
`databricks bundle destroy -t dev --var schema_suffix=pr_<n> --auto-approve` as Devin.
Every new push needs a fresh `dev_verify`. PR CI reads evidence as Devin-CI; it never
deploys or runs jobs. Run URLs are saved to `out/run-urls.txt` and the evidence row.
