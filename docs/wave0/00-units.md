# Wave 0: unit list, dependencies, merge order and builder notes

One unit = one PR = one `registry.json` entry (or a platform change with no registry entry). Owner waves: **Platform**, **Silver** (bronze landing + Lakeflow silver per entity), **Gold** (dims, facts, reports).

Schemas: `bronze.` / `silver.` / `gold.` below mean `${var.bronze_schema}` etc. (dev `bronze_pr_<n>` ..., prod `bronze` / `silver` / `gold`, Terraform-managed). Dev job names carry `${var.job_name_prefix}` (`[pr_<n>] `).

Source paths: `L:` = legacy repo `daiwt-02-enterprise-etl`, unprefixed = this repo. Golden is the spec: where repo SQL text and golden differ, match golden (`golden/manifest.json`, `golden/native_result_sets.json`, `golden/native_destination_columns.json`, `golden/outputs/<drop>/`).

## 1. Unit list

| ID | Unit | Wave | Output(s) | Depends on |
|---|---|---|---|---|
| P0 | Per-drop parity contract (derived 18 outputs x 3 drops, incl. `Integration.Lineage` / `Integration.ETL_Cutoff`) | Platform | `src/parity/contract.json` (derived, golden untouched) | – |
| P1 | Parity: `--drop`, contract filter, column normalizers (lineage keys, load timestamps), per-column diff sample | Platform | `src/parity/parity_check.py`, tests | P0 |
| P2 | Drop-parameterised runner + CI (`drop`, `last_cutoff`, `new_cutoff` job params; D0 -> D1 -> D2 replay; changed-unit selection) | Platform | `ci/run_units.py`, `ci/dev_verify.sh`, `ci/classify_changes.py`, `databricks.yml` vars | P1 |
| P3 | Shared framework SQL: SCD2 close-and-insert macro, point-in-time dim-key lookup `(valid_from, valid_to]` with key-0 fallback, golden CSV formatting helpers | Platform | `src/sql/framework/*` | – |
| P4 | Bronze landing + replay: 38 input tables (25 base + 13 `_Archive`) from `/Volumes/daiwt_02_mig_prod/landing/golden/inputs/<drop>/`, D0 full, D1/D2 upsert + `*.deleted.csv` | Platform | `bronze.*` (one table per source) | P2 |
| P5 | Gold seed + Integration tables: seed gold dims/facts and `Integration.Lineage` / `ETL_Cutoff` from D0 outputs; unknown members (key 0); lineage-key allocator | Platform | `gold.integration_lineage`, `gold.integration_etl_cutoff` | P2, P3 |
| P6 | Silver temporal views: `FOR SYSTEM_TIME AS OF` equivalent over base + `_Archive` (`valid_from <= t < valid_to`) | Platform | `silver.v_*_asof` | P4 |
| P7 | Registry ownership + cleanup: registry entries own bronze/silver/helper tables, not just gold outputs | Platform | `src/sql/units/registry.json` schema, `ci/cleanup.py` | P2 |
| P8 | Lakeflow job `wwi_daily_etl`: one DAG (run start -> bronze -> silver -> dims -> facts -> reports -> run end) | Platform | `resources/wwi_daily_etl.job.yml` | all S/G units |
| P9 | UC governance: grants, tags, comments, expectations/quarantine conventions; prod via Devin-CI / Terraform only | Platform | bundle permissions; Terraform PR in `dbx-data-platform-infra` if schema-level grants change (approval-gated) | P2 |
| P10 | Docs: README + golden contract notes | Platform | `README.md`, `docs/` | P8 |
| S01 | City | Silver | `silver.city_changes` (+ quarantine) | P4, P6 |
| S02 | Customer | Silver | `silver.customer_changes` | P4, P6 |
| S03 | Employee | Silver | `silver.employee_changes` | P4, P6 |
| S04 | Payment Method | Silver | `silver.payment_method_changes` | P4, P6 |
| S05 | Stock Item | Silver | `silver.stock_item_changes` | P4, P6 |
| S06 | Supplier | Silver | `silver.supplier_changes` | P4, P6 |
| S07 | Transaction Type | Silver | `silver.transaction_type_changes` | P4, P6 |
| S08 | Movement | Silver | `silver.movement_changes` | P4 |
| S09 | Order | Silver | `silver.order_changes` | P4, P6 |
| S10 | Purchase | Silver | `silver.purchase_changes` | P4, P6 |
| S11 | Sale | Silver | `silver.sale_changes` | P4, P6 |
| S12 | Stock Holding | Silver | `silver.stock_holding_snapshot` | P4 |
| S13 | Transaction | Silver | `silver.transaction_changes` | P4, P6 |
| G01 | dim_city | Gold | `gold.dim_city` | P3, P5, S01 |
| G02 | dim_customer | Gold | `gold.dim_customer` | P3, P5, S02 |
| G03 | dim_date | Gold | `gold.dim_date` | P5 |
| G04 | dim_employee | Gold | `gold.dim_employee` | P3, P5, S03 |
| G05 | dim_payment_method | Gold | `gold.dim_payment_method` | P3, P5, S04 |
| G06 | dim_stock_item | Gold | `gold.dim_stock_item` | P3, P5, S05 |
| G07 | dim_supplier | Gold | `gold.dim_supplier` | P3, P5, S06 |
| G08 | dim_transaction_type | Gold | `gold.dim_transaction_type` | P3, P5, S07 |
| G09 | fact_movement | Gold | `gold.fact_movement` | S08, G02, G06, G07, G08 |
| G10 | fact_order | Gold | `gold.fact_order` | S09, G01, G02, G04, G06 |
| G11 | fact_purchase | Gold | `gold.fact_purchase` | S10, G06, G07 |
| G12 | fact_sale | Gold | `gold.fact_sale` | S11, G01, G02, G04, G06 |
| G13 | fact_stock_holding | Gold | `gold.fact_stock_holding` | S12, G06 |
| G14 | fact_transaction | Gold | `gold.fact_transaction` | S13, G02, G05, G07, G08 |
| G15 | report_sale_by_day | Gold | `gold.report_sale_by_day` | G12 |
| G16 | report_customer_history | Gold | `gold.report_customer_history` | G02 |

40 units: 11 Platform, 13 Silver, 16 Gold. Dims do not depend on each other; facts do not depend on each other (see `01-inventory-dependency-graph.md` §3).

## 2. Merge order

1. **Platform foundation (serial):** P0 -> P1 -> P2 -> P7, with P3 in parallel.
2. **Platform data plumbing:** P4 -> P6; P5 (after P2, P3); P9 any time after P2.
3. **Silver (parallel, 13 PRs):** S01–S13 once P4/P6 are on `main`. Each Silver PR may also carry its Gold dim/fact if the same owner builds both; otherwise Gold follows.
4. **Gold dims (parallel):** G01–G08.
5. **Gold facts (parallel, each gated on its dims):** G09–G14.
6. **Reports:** G15 after G12; G16 after G02.
7. **Orchestration + docs:** P8 (single Lakeflow job wiring all units), then P10.
8. **Promotion:** `deploy-prod` by Devin-CI after all units pass strict per-drop parity in dev.

Within a wave, merge in legacy order to keep lineage keys aligned: City, Customer, Employee, Payment Method, Stock Item, Supplier, Transaction Type, Movement, Order, Purchase, Sale, Stock Holding, Transaction.

## 3. Builder notes per unit

### Platform

- **P0**: Derive from `golden/manifest.json` without editing golden. 18 outputs (16 manifest `tables` + `Integration.Lineage` + `Integration.ETL_Cutoff`) x D0/D1/D2 = 54 entries, each with path, sha256, row count, ordered columns. Nondeterministic list: `*.lineage_key` (deterministic here: 1–13 / 14–26 / 27–39), `Lineage.data_load_started|completed`. Dims' `valid_from/valid_to` are deterministic given inputs; do not exclude them.
- **P1**: Keep un-migrated outputs as SKIP, registered outputs strict. Normalize only `data_load_started/completed` (wall clock). Compare as golden CSV strings (style 121 datetimes with 7 fractional digits, style 23 dates, decimals at declared scale, `\N` = NULL). Make `--suffix` required. Avoid loading whole large tables into driver memory (fact_sale/fact_movement); compare via SQL `EXCEPT ALL` on staged golden.
- **P2**: Job params `drop`, `last_cutoff`, `new_cutoff` from `manifest.drops[d]` (D1 cutoff 2016-06-14, D2 2016-06-21). `dev_verify` replays D0 -> D1 -> D2 in order and runs parity after each drop. Use `classify_changes` output to run only changed units plus downstream dependants.
- **P3**: SCD2 macro: per business key close the open row with `valid_to = MIN(staged valid_from)`, then insert every staged version in staged order. Lookup: `natural_id =` and `ts > valid_from AND ts <= valid_to`, first by `valid_from`, else 0. Formatting helpers render golden CSV conventions.
- **P4**: Bronze = append-only landing of raw CSV (all STRING) plus a typed current-state table per source. D0 is full; D1/D2 apply `<table>.deleted.csv` first (PK identity; whole-row multiset for no-PK tables), then upserts. Header-only files mean no change. Record `_drop`, `_file`, `_ingested_at`.
- **P5**: D0 gold is a restored baseline, not an ETL product: seed gold dims/facts and Integration tables from `golden/outputs/D0/tables/*` (D0 parity is then a seed check). D1/D2 run incrementally. Lineage: one row per entity per run in legacy entity order. ETL_Cutoff: 13 entity rows advance to the run cutoff; the `Date` row is unchanged. Surrogate keys continue from the D0 seed high-water mark.
- **P6**: Silver views give `AS OF t` over base + `_Archive` using `[ValidFrom, ValidTo)`. Dimension extracts walk every `ValidFrom` change in `(last_cutoff, new_cutoff]` across all driving tables and re-read the entity as of each change.
- **P7**: Registry entry fields: `job`, `outputs` (gold), `owns` (bronze/silver/helper tables), `depends_on`. `cleanup.py` drops `owns` + `outputs` in dev; in prod drops tables only, never schemas (Terraform-managed).
- **P8**: One Lakeflow job: `run_start` (lineage rows) -> `bronze_replay` -> Lakeflow pipeline for silver (expectations + quarantine tables) -> dims (parallel) -> facts (parallel, gated) -> reports -> `run_end` (complete lineage, advance cutoffs). Off-workflow legacy jobs are re-homed or dropped per `04-orchestration-mapping.md`.
- **P9**: Grants via bundle permissions in dev; prod grants/schemas through Terraform PR only, approval-gated. Silver expectations: `expect_or_drop` to quarantine for PK nulls, FK/type casts; never change row counts that golden depends on (quarantine must be empty for golden inputs).
- **P10**: Document drop replay, parity contract and the per-drop CI flow.

### Silver (per entity)

Each unit reproduces the legacy `Integration.Get<X>Updates` result set for `(last_cutoff, new_cutoff]` as a silver table, with expectations and a quarantine table. Use the golden result-set column list/order (`golden/native_result_sets.json`) and destination mapping (`golden/native_destination_columns.json`); mapping is positional.

- **S01 City**: Cities + StateProvinces + Countries (+ archives). Change events = union of `ValidFrom` across the three driving tables; one staged version per event.
- **S02 Customer**: Customers + CustomerCategories + BuyingGroups + People (primary contact) (+ archives). Bill-to customer name resolved as of the change. Match the golden `dim_customer` row set (403 rows incl. key 0 across all drops); validate staged rows against it.
- **S03 Employee**: People with `IsEmployee = 1` (+ archive); binary columns render as uppercase hex.
- **S04 Payment Method / S07 Transaction Type**: single temporal table each; simplest SCD2 sources, good first PRs to prove P3/P6.
- **S05 Stock Item**: StockItems + Colors + PackageTypes (selling/buying) (+ archives). NULL/default handling per `03-business-rules.md`.
- **S06 Supplier**: Suppliers + SupplierCategories + People (primary contact) (+ archives).
- **S08 Movement**: StockItemTransactions by `LastEditedWhen` window. The staged timestamp column (`Last Modified When` in golden metadata) carries the transaction occurrence time positionally; key lookups use that value.
- **S09 Order**: Orders + OrderLines + PackageTypes + Customers; line-level window `MAX(o.LastEditedWhen, ol.LastEditedWhen)`; totals ROUND to 2 dp as in the extract.
- **S10 Purchase**: PurchaseOrders + PurchaseOrderLines + StockItems + PackageTypes. Staged rows accumulate across drops within the legacy control flow (the Purchase load's clear step targets `Order_Staging`); reproduce staging content exactly as the golden capture does (`golden/capture_sqlserver.py:36-39`).
- **S11 Sale**: Invoices + InvoiceLines + StockItems + PackageTypes + Customers; Total Excluding Tax / Tax Amount / Profit rounding as in `GetSaleUpdates`.
- **S12 Stock Holding**: StockItemHoldings full snapshot every run (no window).
- **S13 Transaction**: CustomerTransactions + SupplierTransactions (+ Invoices for invoice id); two result-set halves unioned.

### Gold dims

- **G01, G02, G04–G08**: P3 SCD2 macro on the silver change set; key = next value after the seeded high-water mark, assigned in staged order; key 0 unknown member from seed. Stage every version; no no-op suppression (golden includes all staged versions).
- **G03 dim_date**: 14 golden columns only (not the ~62-column repo DDL). 1,461 rows 2013-01-01..2016-12-31, identical in all drops; seed from D0 output and keep static. Fiscal attributes: copy golden values; do not port `GenerateDateDimensionColumns` literally.

### Gold facts

- **G09 fact_movement**: MERGE on WWI Stock Item Transaction ID (update in place, insert new).
- **G10 fact_order / G11 fact_purchase / G12 fact_sale**: delete all rows for staged document IDs, then insert staged rows with new surrogate keys after the high-water mark. Key order = staged order.
- **G13 fact_stock_holding**: truncate and reload every run; keys restart at 1 each run (227 rows). Stock Item key = latest version (`TOP 1 ORDER BY Valid To DESC`, no time window). `Last Cost Price` keeps the decimal value as rendered in golden CSV.
- **G14 fact_transaction**: insert-only; each staged row is appended with a new key, existing rows are never updated or deleted.
- All facts except G13: dimension keys via P3 lookup `(valid_from, valid_to]` on `Last Modified When`, default 0. Undelivered/unpicked dates stay NULL (`\N`).

### Reports

- **G15 report_sale_by_day**: `SELECT invoice_date_key, COUNT(*) AS sale_rows, SUM(total_including_tax) AS total_including_tax FROM gold.fact_sale GROUP BY invoice_date_key` (all rows, incl. key-0 customers). Render sums at declared scale.
- **G16 report_customer_history**: `customer_key, wwi_customer_id, customer, valid_from, valid_to` from `gold.dim_customer` incl. key 0. Parity hashes sorted rows, so physical order is irrelevant; if order is ever asserted, golden file order is string order of the formatted key.
