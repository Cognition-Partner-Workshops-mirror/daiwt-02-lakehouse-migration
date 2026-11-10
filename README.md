# DAIWT-02 Lakehouse Migration

Wide World Importers SQL Server / SSIS migration to Databricks bronze, silver
and gold. Golden captures define correctness: replay D0, D1 and D2 in order,
then compare registered outputs for each drop. Never edit `golden/` or
`/Volumes/daiwt_02_mig_prod/landing/golden/` to make parity pass.

## Guides

- [Drop replay](docs/drop-replay.md): bronze landing, cutoff windows and D0 seed.
- [Parity contract](docs/parity-contract.md): exact comparison, timestamps and SKIPs.
- [CI flow](docs/ci-flow.md): selection, per-drop evidence, carry-over and teardown.
- [Registry](docs/registry.md): jobs, ownership and dependencies.
- [Governance](docs/governance.md): metadata and non-destructive quarantine.
- [Silver temporal views](docs/silver-temporal-views.md).
- [docs/daily-etl.md (P8)](docs/daily-etl.md): daily DAG guide, maintained by P8
  (PR #39; not yet on main when this guide was written).
- [Wave 0 unit map](docs/wave0/00-units.md) and
  [target model](docs/wave0/02-target-model.md).

## Layout and targets

| Path | Purpose |
| --- | --- |
| `databricks.yml`, `resources/` | Bundle targets, jobs and pipelines |
| `src/bronze/` | Golden input landing and snapshot replay |
| `src/sql/units/registry.json` | Job keys, table ownership and dependencies |
| `src/sql/framework/`, `src/governance/` | Shared SQL helpers and quality conventions |
| `src/parity/contract.json`, `src/parity/parity_check.py` | 18 outputs per drop and warehouse-side parity |
| `ci/`, `.github/workflows/` | Selection, runners, evidence and scoped cleanup |
| `tests/` | Offline tests (unit-local tests also live under `src/`) |

Dev uses `daiwt_02_mig_dev` and bundle-owned `bronze_pr_<n>`, `silver_pr_<n>`,
`gold_pr_<n>` schemas. Prod uses `daiwt_02_mig_prod` and fixed `bronze`, `silver`,
`gold`; production schemas and `landing` are Terraform-managed, not bundle-owned.
The SQL warehouse is `6dc4b80538621b7b`. Both targets grant the deploying principal
`CAN_MANAGE` and `users CAN_VIEW`; never add resource-level `users` permissions.

## Local checks

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -q
databricks current-user me
databricks bundle validate --strict -t dev --var schema_suffix=pr_1
databricks bundle validate --strict -t prod
```

Use Devin's default CLI profile and unified SDK authentication locally. Actions
uses Devin-CI with GitHub OIDC, repository variables `DATABRICKS_HOST` /
`DATABRICKS_CLIENT_ID`, and the matching `dev` or `prod` environment. Validation
is read-only; production execution belongs to the production workflow, not local
builder checks. Read [CI flow](docs/ci-flow.md) before deploying or cleaning up.
