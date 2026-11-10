# Wave 0 discovery: WWI daily ETL -> Databricks lakehouse

Scope: the 13-entity WWI daily ETL (8 dimensions, 6 facts, 2 Integration tables) and the 2 reports (customer-history, sale-by-day). Target: bronze -> Lakeflow silver (expectations + quarantine) -> gold dims/facts + reports, one Lakeflow job, UC governance, promoted to prod by Devin-CI.

| Doc | Contents |
|---|---|
| [00-units.md](00-units.md) | Unit list (Platform / Silver / Gold), dependencies, merge order, builder notes per unit |
| [01-inventory-dependency-graph.md](01-inventory-dependency-graph.md) | In-scope SSIS package, procs, tables; per-entity chains; daily control flow; dependency graph; load order |
| [02-target-model.md](02-target-model.md) | Bronze/silver/gold tables mapped to golden outputs; keys, SCD2, unknown members; rationalisation |
| [03-business-rules.md](03-business-rules.md) | `Get*Updates` / `MigrateStaged*` rules, report SQL, rounding/NULL/default-member rules, golden formatting |
| [04-orchestration-mapping.md](04-orchestration-mapping.md) | `DailyETLMain` + SQL Agent jobs -> one Lakeflow job DAG; off-workflow jobs re-homed or dropped |
| [05-shared-library-ddl-harness.md](05-shared-library-ddl-harness.md) | Shared framework/DDL, type mapping, D0..D2 replay in bronze, harness changes for per-drop parity |

Conventions: `[V]` verified against code or golden files, `[C]` read from code only, `[I]` inference. Golden (`golden/`, `/Volumes/daiwt_02_mig_prod/landing/golden/`) is the spec and is never modified.
