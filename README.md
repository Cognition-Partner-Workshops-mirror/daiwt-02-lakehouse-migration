# DAIWT-02 Lakehouse Migration

SQL-unit migration scaffold for the Wide World Importers estate:
SQL Server `WideWorldImporters`, `WideWorldImportersDW` and SSIS packages.

## Layout

| Path | Purpose |
| --- | --- |
| `databricks.yml` | Dev and production bundle targets |
| `src/sql/units/` | Migrated SQL files and `registry.json`; currently zero units |
| `src/parity/parity_check.py` | Golden CSV multiset comparison using SDK Files and SQL Statements APIs |
| `ci/dev_verify.sh` | Deploy dev, run all registered unit jobs and parity |
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
   `databricks.yml`. Pass catalog and layer schema variables to the job.
2. Register the bundle job key and all produced golden tables:

   ```json
   {"load_sale": {"job": "load_sale", "outputs": ["gold.fact_sale"]}}
   ```

3. Add offline tests. Each output has exactly one producer. Registry insertion order
   determines job execution order; list upstream units before their consumers.

`classify_changes.sh` compares `BASE_SHA`/`HEAD_SHA` (default `origin/main`/`HEAD`).
SQL changes select their unit; registry or shared framework changes select all units.
README, workflow and test-only edits do not select unit jobs. Classification is recorded
as diagnostic evidence. Dev verification runs all registered units so a fresh PR schema
contains every registered output checked by parity.

## CI

| Workflow | Trigger and behavior |
| --- | --- |
| `pr.yml` | PRs to `main` or `pre-migration`; offline tests, strict dev validation, full registered-unit verification, sticky parity attestation and evidence artifact |
| `deploy-prod.yml` | Push to `main` or manual; strict prod validation, bundle deployment, all registered production units and prod parity |
| `reset.yml` | Manual `target=dev|prod`; runs only if `confirm` exactly matches target |
| `dev-sweep.yml` | Manual; removes dev PR deployments for closed PRs |

Reset dev removes only `bronze|silver|gold_pr_<n>` schemas bearing this bundle's
comment marker and destroys their suffix-scoped bundle jobs. Sweep uses the same
scope but only for closed PR numbers obtained from GitHub. Production reset destroys
this bundle's production jobs and drops only tables listed in the units registry;
it leaves Terraform-owned schemas in place. `landing` and its golden volume are
never cleanup targets. These destructive scripts must run as Devin-CI, the deployment
principal, so bundle state resolves to the same workspace user root as deployment.
There are currently no production tables or jobs to reset.

## Local checks

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -q
databricks bundle validate --strict -t prod
databricks bundle validate --strict -t dev --var schema_suffix=pr_1
python src/parity/parity_check.py --profile DEFAULT
python ci/attest.py
```

For an authorized dev deployment, run `bash ci/dev_verify.sh pr_<n>`.
Parity writes `out/parity.json`; attestation writes `attestation.md`. Missing or
inconsistent results and any FAIL cause attestation to exit nonzero.
