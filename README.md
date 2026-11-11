# daiwt_02

## Migration CI

Install `python -m pip install -r requirements-dev.txt`. Add a directory under
`src/units/<name>/` with `unit.yml` and ordered SQL files; no shared registry.
Run `migration-ci fetch-goldens` then `migration-ci check` (or
`migration-ci check --goldens /path/to/local/goldens`). `pytest --goldens PATH`
runs the same offline gate. Plain pytest validates structure without remote data.
PRs run offline checks only. Manually dispatch integrate for dev integration,
then promote for production. Reset destroys only the selected bundle and its
bronze/silver/gold/verification schemas, never the landing golden volume.
Framework fixes propagate by running `migration-ci sync .` from a new framework
version and opening a PR for the changed vendored files.

## Clean demo baseline

Framework version: **1.0.5**. No migrated units are present. The legacy source is
[daiwt-02-enterprise-etl](https://github.com/Cognition-Partner-Workshops-mirror/daiwt-02-enterprise-etl).
Historical source, business-rule and orchestration references remain in `docs/`.

`golden/manifest.json` is unchanged. `estate.yml` points to the separate normalized
fixture volume `/Volumes/daiwt_02_mig_prod/migration_ci_fixtures/golden`; its
manifest is recorded in `golden/normalized-manifest.json`. This copy contains only
D0, uses list-based file metadata, maps all 18 output CSVs to tables using the
legacy parity contract, and uses lowercase source basenames for bronze tables.
All 56 CSV files retain their original contents and SHA-256 checksums. The
original `/Volumes/daiwt_02_mig_prod/landing/golden` is never overwritten.

This baseline intentionally configures **D0 only**. It does not implement or run
D1/D2 cumulative replay. Understand and Migrate are separate later demo steps.
The empty-estate offline gate validates framework wiring, not business parity.
