# WWI shared library, DDL and parity harness — Wave 0 workstream (e)

Repos read (read-only):
- Legacy `Cognition-Partner-Workshops-mirror/daiwt-02-enterprise-etl` @ `bb42a64` (the commit the golden manifest records in `golden/manifest.json` → `source.legacy_commit`).
- Migration `Cognition-Partner-Workshops-mirror/daiwt-02-lakehouse-migration` `main` @ `dd46399`.

Paths are relative to each repo root. Legacy paths start with `wwi-*`, `sqlserver/` or `ssis/`. Migration paths start with `golden/`, `src/`, `ci/`, `.github/`, `tests/` or `databricks.yml`.
Tags: **[V]** means verified by reading the file or counting golden CSV rows. **[I]** means inferred and needs a check.

---

## 1. In-scope vs out-of-scope legacy

The golden capture ran the **native** WWI daily ETL and nothing else. `golden/capture_sqlserver.py:661-700` calls the following, in the `ENTITIES` order at lines 21-35:
1. `Integration.PopulateDateDimensionForYear`.
2. Then, for each of the 13 entities: `Integration.GetLineageKey`, then `DELETE` staging, then `GetLastETLCutoffTime`, then `INSERT … EXEC WideWorldImporters.Integration.Get<X>Updates`, then `Integration.MigrateStaged<X>Data`.

This is the same call set as `wwi-ssis/wwi-ssis/DailyETLMain.dtsx`. In that file, `GetLineageKey` and `GetLastETLCutoffTime` each appear 13×, plus one `PopulateDateDimensionForYear`, one `Get*Updates` and one `MigrateStaged*` per entity **[V]**.

| Area | Path | In scope? | Notes |
|---|---|---|---|
| DW DDL: Dimension, Fact, Integration tables | `wwi-dw-ssdt/wwi-dw-ssdt/{Dimension,Fact,Integration}/Tables/*.sql` | **IN** | 8 dims, 6 facts, 13 `*_Staging`, `ETL Cutoff`, `Lineage` (§2) |
| DW sequences | `wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/*.sql` (8 files) and `Sequences/Stored Procedures/ReseedAllSequences.sql`, `ReseedSequenceBeyondTableValues.sql` | **IN** (as a key-generation contract) | All 8 are `INT START WITH 1 INCREMENT BY 1`: City, Customer, Employee, Lineage, PaymentMethod, StockItem, Supplier, TransactionType |
| DW ETL procs | `wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/{GetLineageKey,GetLastETLCutoffTime,MigrateStaged*Data,PopulateDateDimensionForYear}.sql` | **IN** | 13 MigrateStaged procs, 3 helpers |
| DW function | `wwi-dw-ssdt/wwi-dw-ssdt/Integration/Functions/GenerateDateDimensionColumns.sql` | **IN, but diverges from the live estate** | The repo version emits about 62 columns. The live DW and golden `dim_date` have 14 columns (§2.4). |
| Unknown-member and reset seed | `wwi-dw-ssdt/wwi-dw-ssdt/Application/Stored Procedures/Configuration_ReseedETL.sql:8-77` | **IN** | Key-0 "Unknown" rows for all 7 SCD dims. ETL cutoff reset to `2012-12-31`. |
| OLTP extract procs | `wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/Get*Updates.sql` (13) | **IN** (logic owned by entity workstreams) | Use `FOR SYSTEM_TIME AS OF` and `*_Archive` scans (§4.3) |
| OLTP temporal source tables | `wwi-ssdt/wwi-ssdt/{Application,Purchasing,Sales,Warehouse}/Tables/*.sql` | **IN** (bronze schema source) | 17 are `SYSTEM_VERSIONING = ON` (§4.3) |
| SSIS package | `wwi-ssis/wwi-ssis/DailyETLMain.dtsx`, `Project.params`, `*.conmgr` | **IN** (orchestration only) | Becomes the single Lakeflow job |
| DW storage, partitions, in-memory | `wwi-dw-ssdt/wwi-dw-ssdt/Storage/*.sql` (PF_Date, PS_Date, USERDATA, MemoryOptimized) | **DROP** | Partitioning by `Date Key` becomes optional Delta liquid clustering |
| DW security | `wwi-dw-ssdt/wwi-dw-ssdt/Security/*.sql` | **DROP**, replaced by UC grants (Terraform-owned) | |
| DW app procs | `Application/Stored Procedures/Configuration_ApplyPolybase.sql`, `Configuration_PopulateLargeSaleTable.sql`; `PostDeploymentScripts/Script.PostDeployment1.sql` | **DROP** (except the ReseedETL semantics above) | |
| Expanded control framework | `sqlserver/control/01..07_*.sql` (44 `etl.*` tables plus `err.*`) and `sqlserver/control/procedures/etl.usp_*.sql` (17) | **OUT** | The native package references none of them **[V]**. Manifest `skipped_branches` lists them as missing in the live DB. Of the expanded `ssis/` packages, 218 files reference `etl.usp_*`; none is in scope. |
| Expanded staging layer | `sqlserver/staging/{tables,procedures,functions,views}` (`raw.*`, `stg.*`, `work.*`, `ref.*`, and functions `stg.ufn_CleanString`, `ufn_SafeDate`, `ufn_SafeDecimal`, `ufn_SourceSystemKey`, `ufn_StandardizePhone`, `ufn_StandardizePostalCode`) | **OUT** | Belongs to `WideWorldImporters_Staging`, which was never hosted (legacy `README.md`, table "Systems and consumers"). Nothing in scope calls these UDFs. |
| Expanded warehouse and procs | `sqlserver/warehouse/**`, `sqlserver/procedures/**` (e.g. `Integration.usp_LoadFactSale.sql`, `usp_MigrateStagedCustomerDataV2.sql`), `sqlserver/views/Report.*`, `sqlserver/agent/*`, `sqlserver/reference/*`, `sqlserver/oltp/**` | **OUT** | Same-named "V2" procs exist but are not the native path. Builders must not port them. |
| `ssis/00..99_*` (≈205 packages), `oracle/`, `wwi-ssasmd/`, `power-bi-dashboards/`, `wwi-app/`, `workload-drivers/`, `generators/` | — | **OUT** | |

### What becomes shared Databricks code vs what drops away

| Legacy construct | Databricks equivalent (shared, platform-owned) |
|---|---|
| `Integration.Lineage` and `Sequences.LineageKey`, `GetLineageKey`, the "complete lineage" UPDATE at the end of every MigrateStaged proc | `common/lineage.sql` (or `.py`): `start_lineage(table_name, cutoff)`, which returns a deterministic key, and `complete_lineage(key)`. Written to `gold.integration_lineage`. |
| `Integration.[ETL Cutoff]` and `GetLastETLCutoffTime`, the cutoff UPDATE at the end of every MigrateStaged proc | `common/cutoff.sql`: a table `gold.integration_etl_cutoff` (14 rows, including `Date`) plus get/set helpers. Note that the `Date` row stays at `2012-12-31` (golden `outputs/D*/tables/Integration.ETL_Cutoff.csv`) **[V]**. |
| `Sequences.*Key` with `DEFAULT NEXT VALUE FOR` (dims), `IDENTITY(1,1)` (facts, staging) | A deterministic surrogate-key macro: `max(existing key) + row_number() over (<legacy insertion order>)`. Do **not** use Delta `GENERATED … AS IDENTITY`, which is not gap-free or deterministic. Required because surrogate keys are compared verbatim (§5.4). |
| `@EndOfTime = '99991231 23:59:59.9999999'` and `@StartOfTime = '20130101'` (`Configuration_ReseedETL.sql:9-10`; `MigrateStagedCustomerData.sql:9`) | Shared constants: `END_OF_TIME` string `9999-12-31 23:59:59.9999999` and `START_OF_TIME` `2013-01-01 00:00:00.0000000`. |
| Unknown members (key 0) from `Configuration_ReseedETL.sql:30-77` | `common/unknown_members.sql`, seeded once per gold schema. Golden row example: `0,0,Unknown,N/A,…,\N,0,2013-01-01 00:00:00.0000000,9999-12-31 23:59:59.9999999,0` (`outputs/D2/tables/Dimension.City.csv` line 2). |
| SCD2 close-off pattern, repeated in 7 dim procs (`MigrateStagedCustomerData.sql:19-40` is typical) | A shared SCD2 macro: close the open row with `Valid To = MIN(staged Valid From)` per business key, then insert all staged rows. |
| Point-in-time dim key lookup, repeated in 5 fact procs (`MigrateStagedSaleData.sql:76-107`: `Last Modified When > Valid From AND <= Valid To ORDER BY Valid From`, `COALESCE(…,0)`) | A shared `lookup_dim_key(dim, bk, as_of)` macro (a range join). Note the half-open interval `(Valid From, Valid To]`, not `[ , )`. StockHolding uses a different rule: latest row by `Valid To DESC` (`MigrateStagedStockHoldingData.sql:160-165`). |
| Fact upsert by business key (DELETE by `WWI Invoice ID` then INSERT, `MigrateStagedSaleData.sql:111-124`) and full replace (`TRUNCATE` for Stock Holding, `MigrateStagedStockHoldingData.sql:169`) | Shared `replace_by_key` and `full_replace` patterns: Delta `MERGE … WHEN NOT MATCHED BY SOURCE` scoped to the keys, or `DELETE` plus `INSERT` in one task. |
| `PopulateDateDimensionForYear` with `GenerateDateDimensionColumns` | A shared date-dimension generator built from the 14 **live** columns (§2.4), not the repo function. |
| `FOR SYSTEM_TIME AS OF @t` over OLTP temporal tables | A shared `as_of(table, t)` view or macro over bronze `current ∪ _Archive` (§4.3). |
| SQL Server `CONVERT` formatting styles used by the golden (§3) | Shared format UDFs, e.g. `fmt_dt7(ts_string)`, used only if gold stores native types (§3 decision). |
| `etl.*`, `err.*`, `stg.ufn_*`, partition schemes, `Security/*`, in-memory, Polybase | **Drop.** Use Lakeflow expectations, quarantine and job run history instead. |

---

## 2. DW DDL (in-scope tables)

Source: `wwi-dw-ssdt/wwi-dw-ssdt/<Schema>/Tables/<Table>.sql`. The column lists below are verified against the golden `source_columns` in `golden/manifest.json` → `files["outputs/D2/tables/*.csv"].source_columns`, except Dimension.Date (§2.4).

### 2.1 Dimensions (all SCD2; PK is the surrogate key; key 0 is Unknown)

| Table | Surrogate key | Business key | Other columns: type and nullability |
|---|---|---|---|
| Dimension.City | `City Key INT NOT NULL DEFAULT NEXT VALUE FOR Sequences.CityKey` | `WWI City ID INT NN` | City nvarchar(50) NN; State Province nvarchar(50) NN; Country nvarchar(60) NN; Continent nvarchar(30) NN; Sales Territory nvarchar(50) NN; Region nvarchar(30) NN; Subregion nvarchar(30) NN; **Location geography NULL**; Latest Recorded Population bigint NN; Valid From/To datetime2(7) NN; Lineage Key int NN |
| Dimension.Customer | `Customer Key` (seq CustomerKey) | `WWI Customer ID` | Customer nvarchar(100) NN; Bill To Customer nvarchar(100) NN; Category nvarchar(50) NN; Buying Group nvarchar(50) NN; Primary Contact nvarchar(50) NN; Postal Code nvarchar(10) NN; Valid From/To; Lineage Key |
| Dimension.Employee | `Employee Key` (seq EmployeeKey) | `WWI Employee ID` | Employee nvarchar(50) NN; Preferred Name nvarchar(50) NN; Is Salesperson **bit** NN; **Photo varbinary(max) NULL**; Valid From/To; Lineage Key |
| Dimension.Payment Method | `Payment Method Key` (seq) | `WWI Payment Method ID` | Payment Method nvarchar(50) NN; Valid From/To; Lineage Key |
| Dimension.Stock Item | `Stock Item Key` (seq) | `WWI Stock Item ID` | Stock Item nvarchar(100) NN; Color nvarchar(20) NN; Selling/Buying Package nvarchar(50) NN; Brand nvarchar(50) NN; Size nvarchar(20) NN; Lead Time Days int NN; Quantity Per Outer int NN; Is Chiller Stock bit NN; Barcode nvarchar(50) NULL; **Tax Rate decimal(18,3) NN**; Unit Price decimal(18,2) NN; Recommended Retail Price decimal(18,2) NULL; **Typical Weight Per Unit decimal(18,3) NN**; Photo varbinary(max) NULL; Valid From/To; Lineage Key |
| Dimension.Supplier | `Supplier Key` (seq) | `WWI Supplier ID` | Supplier nvarchar(100) NN; Category nvarchar(50) NN; Primary Contact nvarchar(50) NN; Supplier Reference nvarchar(20) NULL; Payment Days int NN; Postal Code nvarchar(10) NN; Valid From/To; Lineage Key |
| Dimension.Transaction Type | `Transaction Type Key` (seq) | `WWI Transaction Type ID` | Transaction Type nvarchar(50) NN; Valid From/To; Lineage Key |
| Dimension.Date | `Date DATE NN` (PK, natural) | — | §2.4 |

None of the DW tables is temporal (`SYSTEM_VERSIONING` is absent in every DW table file) **[V]**. SCD2 is hand-rolled through `Valid From` and `Valid To`.

### 2.2 Facts

All fact keys are `BIGINT IDENTITY(1,1) NOT NULL`. The PK is `(key, date key)`, partitioned on `PS_Date`, except Stock Holding (PK is the key only).

| Table | Key / PK | Dim FKs (nullability) | Measures and attributes |
|---|---|---|---|
| Fact.Movement | Movement Key, Date Key | Date Key date NN; Stock Item Key NN; Customer Key **NULL**; Supplier Key **NULL**; Transaction Type Key NN | WWI Stock Item Transaction ID int NN; WWI Invoice ID int NULL; WWI Purchase Order ID int NULL; Quantity int NN; Lineage Key |
| Fact.Order | Order Key, Order Date Key | City NN; Customer NN; Stock Item NN; Order Date Key date NN; Picked Date Key date NULL; Salesperson NN; Picker **NULL** | WWI Order ID NN; WWI Backorder ID NULL; Description nvarchar(100); Package nvarchar(50); Quantity int; Unit Price dec(18,2); Tax Rate dec(18,3); Total Excl/Tax Amount/Total Incl dec(18,2); Lineage Key |
| Fact.Purchase | Purchase Key, Date Key | Supplier NN; Stock Item NN | WWI Purchase Order ID int NULL; Ordered Outers, Ordered Quantity, Received Outers int NN; Package nvarchar(50); Is Order Finalized bit NN; Lineage Key |
| Fact.Sale | Sale Key, Invoice Date Key | City, Customer, Bill To Customer, Stock Item, Salesperson all NN; Delivery Date Key date NULL | WWI Invoice ID NN; Description; Package; Quantity; Unit Price dec(18,2); Tax Rate dec(18,3); Total Excl, Tax Amount, Profit, Total Incl dec(18,2); Total Dry Items, Total Chiller Items int; Lineage Key |
| Fact.Stock Holding | Stock Holding Key | Stock Item NN | Quantity On Hand int; Bin Location nvarchar(20); Last Stocktake Quantity int; Last Cost Price dec(18,2); Reorder Level, Target Stock Level int; Lineage Key |
| Fact.Transaction | Transaction Key, Date Key | Customer, Bill To Customer, Supplier, Payment Method all **NULL**; Transaction Type NN | WWI Customer/Supplier Transaction ID, WWI Invoice ID, WWI Purchase Order ID all int NULL; Supplier Invoice Number nvarchar(20) NULL; Total Excl, Tax, Total Incl, Outstanding Balance dec(18,2) NN; Is Finalized bit NN; Lineage Key |

FKs to the dims are declared, e.g. `Fact/Tables/Transaction.sql:21-26`. These become UC informational `FOREIGN KEY … NOT ENFORCED` constraints or silver expectations. Unresolved lookups are written as key `0`, not NULL (`COALESCE(…,0)`), so a NULL FK appears only where the staging value was NULL.

### 2.3 Integration tables

| Table | Columns |
|---|---|
| Integration.ETL Cutoff (`Integration/Tables/ETL Cutoff.sql`) | Table Name **sysname** NN PK; Cutoff Time datetime2(7) NN. Golden has 14 rows: 13 entities plus `Date`. |
| Integration.Lineage (`Integration/Tables/Lineage.sql`) | Lineage Key int DEFAULT `NEXT VALUE FOR Sequences.LineageKey` PK; Data Load Started datetime2(7) NN; Table Name sysname NN; Data Load Completed datetime2(7) **NULL**; Was Successful bit NN; Source System Cutoff Time datetime2(7) NN |
| 13 × `Integration.<X>_Staging` | Surrogate `<X> Staging Key` IDENTITY (INT for dims, BIGINT for facts). Dims have the same columns as the target dim minus the surrogate and lineage keys. Fact staging is all-NULLable: the target columns plus `WWI <dim> ID` business keys plus `Last Modified When`. Movement's column is spelled **`Last Modifed When`** (`Integration/Tables/Movement_Staging.sql`; see also `golden/README.md` "Destination columns"). StockHolding_Staging has no Last Modified column. |

Staging tables are **not** golden outputs and need not be persisted. In Databricks they become silver temp views or CTEs per unit.

### 2.4 Dimension.Date: repo DDL vs live

- Repo `Dimension/Tables/Date.sql:1-64` declares about 62 columns (`DateKey`, `Day of Year`, `Beginning of *`, `Year * Key`, …), and `GenerateDateDimensionColumns.sql` derives them **[V]**.
- The live DW, and so the golden, has only 14 columns: `Date, Day Number, Day, Month, Short Month, Calendar Month Number, Calendar Month Label, Calendar Year, Calendar Year Label, Fiscal Month Number, Fiscal Month Label, Fiscal Year, Fiscal Year Label, ISO Week Number` (manifest `source_columns`) **[V]**.
- The repo version is the expanded estate. The **original Microsoft** procedure is what ran live.
- **Treat the golden as the spec**: build the 14-column shape and derive values from golden `dim_date`.
- Coverage is 1461 rows, 2013-01-01..2016-12-31, in every drop. `PopulateDateDimensionForYear(cutoff.year)` is idempotent (`IF NOT EXISTS`) **[V]**.

---

## 3. T-SQL → Delta type mapping (parity-safe)

Parity compares **SQL Statements API string values** against golden strings exactly (`src/parity/parity_check.py:22-25` `digest`, `:105-118` `rows`, `:127-134` `actual`). Golden formatting comes from `golden/capture_sqlserver.py:315-334` `format_sql` and manifest `csv_conventions`.

| T-SQL type (where used) | Golden string format (example) | Recommended Delta type in gold | Parity notes |
|---|---|---|---|
| `int`, `bigint` (keys, quantities) | `69644` | `INT` / `BIGINT` | Exact |
| `bit` (Is Salesperson, Is Chiller Stock, Is Order Finalized, Is Finalized, Was Successful) | `0` / `1` | **`INT` (or `TINYINT`) holding 0/1**, plus a `CHECK (col IN (0,1))` | Delta `BOOLEAN` renders `true`/`false` through the Statements API, so it would FAIL strict parity **[I: verify the API rendering once]**. Decision for the lead: native BOOLEAN plus a harness formatter, or INT. INT keeps the harness formatting-free. |
| `decimal(18,2)` | `2300.00`, `0.00` | `DECIMAL(18,2)` | The Statements API keeps the declared scale **[I: verify]**. Never compute via DOUBLE. |
| `decimal(18,3)` (Tax Rate, Typical Weight Per Unit) | `15.000`, `0.300` | `DECIMAL(18,3)` | Same |
| `money` / `smallmoney` | `CONVERT(varchar(50), money)` gives 2 dp | `DECIMAL(19,4)` in bronze would print 4 dp, so **cast to DECIMAL(18,2)** where a money value flows into a dec(18,2) column | No `money` column exists in any in-scope DW table **[V: §2]**. OLTP in-scope tables use decimal (e.g. StockItems `UnitPrice 25.00`, `TaxRate 15.000`) **[V for StockItems]**. |
| `date` (all `* Date Key`, Dimension.Date.Date) | `2013-01-01` (style 23) | `DATE` | Exact **[I: API renders `yyyy-MM-dd`]** |
| `datetime2(7)` (Valid From/To, Cutoff Time, Lineage times, Last Modified When) | `2013-01-01 00:00:00.0000000`, `9999-12-31 23:59:59.9999999`, `2016-06-06 12:36:16.5814702` (style 121, **7 fractional digits**) | **`STRING` in canonical `yyyy-MM-dd HH:mm:ss.fffffff` form** for every gold column that is a golden output. Optionally add a parallel `TIMESTAMP_NTZ` column in silver for logic. | Delta TIMESTAMP and TIMESTAMP_NTZ have **microsecond** precision, and the API renders an ISO `T…` form. The end-of-time sentinel `…59.9999999` cannot round-trip. Source temporal values carry 7 digits. Comparing strings also preserves the half-open interval logic exactly. |
| `nvarchar(n)`, `nvarchar(max)`, `sysname` | raw text, `\N` = NULL, `''` ≠ NULL | `STRING` | No length enforcement. Add expectations `length(col) <= n` in silver if wanted. SQL Server CI collation affects joins and GROUP BY **[I: business keys are ints, so low risk]**. |
| `geography` (City.Location; OLTP Cities.Location, StateProvinces.Border, Countries.Border) | `STAsText()` WKT, e.g. `POINT (-78.651695 42.1083969)` | `STRING` (WKT passed through from bronze) | Do not round-trip through `ST_GeomFromWKT`/`ST_AsText`, which would reformat the numbers. Countries.Border is MULTIPOLYGON text of MB size (D0 Countries CSV is 3.4 MB for 190 rows) **[V]**. |
| `varbinary(max)` (Employee.Photo, Stock Item.Photo; OLTP People.Photo, HashedPassword) | `CONVERT(varchar(max), x, 2)`, uppercase hex without `0x` | `STRING` (hex) | In golden, every `photo` is `\N` (D2 Employee 214/214, Stock Item 672/672) **[V]**. BINARY would render base64. |
| NULL | `\N` | — | A literal `\N` string cannot be told apart from NULL (`golden/README.md`) |

**Rule for builders:**
- Gold tables that are parity targets use the column order and snake_case names of manifest `tables[].columns`.
- The types are exactly those above.
- Bronze lands every column as `STRING` exactly as in the CSV. Casting to native types happens in silver.
- Gold casts back to the parity-safe types.

---

## 4. Golden drop replay: bronze ingestion spec

### 4.1 Facts

From `golden/README.md` and the manifest `input_semantics`, `batch_semantics`, `files` **[V]**:
- `batch_semantics = "cumulative"`. Outputs are **full** every drop and reflect the DW state after that drop's ETL.
- Each drop has 38 input files: 25 base OLTP tables and 13 `_Archive` history tables.
  - Base tables have `key_semantics = primary_key`, e.g. `Cities: ["CityID"]`.
  - `_Archive` tables have `primary_key = []` and `key_semantics = whole_row_multiset`.
- D0 files: `input_kind = base`, `previous_input = null`, full rows. Row counts are in `files[*].row_count`.
- D1 and D2 files: `input_kind = upsert`, `previous_input = inputs/D(n-1)/<same file>`. Each holds inserted or changed whole rows. Unchanged tables are header-only (0 rows; e.g. D1 has 0 rows for Cities, Customers, StockItems).
- `<db.schema.table>.deleted.csv`:
  - PK columns only for PK tables.
  - Full rows with multiplicity for multiset tables.
  - **No deleted files exist in D1 or D2 today** **[V: `ls golden/inputs/D1 golden/inputs/D2`]**. Ingestion must still support them, because the manifest contract allows them.
- `full_row_count` and `full_sha256` per delta file give the expected reconstructed snapshot size and hash. The hash is computed over the scratch export's **SQL Server row order**, so reproducing the hash needs that ordering. Use row-count plus multiset checks instead.
- Drop windows (`manifest.drops`):

| Drop | Window | New cutoff |
|---|---|---|
| D0 | none: Microsoft backup state, all 13 cutoffs `2016-06-06 12:31:17.0000000`, Date `2012-12-31` | — |
| D1 | 2016-06-07..2016-06-13 | `2016-06-14 00:00:00` |
| D2 | 2016-06-14..2016-06-20 | `2016-06-21 00:00:00` |

  Golden `Integration.ETL_Cutoff` after D2 shows `2016-06-21 00:00:00.0000000` for 13 rows and `2012-12-31…` for Date **[V]**.

### 4.2 Bronze algorithm (per source table T, per drop d in D0, D1, D2)

The reconstruction order matches `golden/capture_sqlserver.py:193-229` `apply_input_chain`: deletes first, then upserts.

1. **Landing:** `read_files('/Volumes/daiwt_02_mig_prod/landing/golden/inputs/<d>/WideWorldImporters.<Schema>.<Table>.csv', format=>'csv', header=>true, nullValue=>'\\N', multiLine=>true, escape=>'"', schema=><all STRING in manifest column order>)`.
   - Encoding is UTF-8 and lines end in LF.
   - JSON columns such as `UserPreferences` and `CustomFields` contain quoted commas and newlines, hence `multiLine`.
   - Use an explicit all-STRING schema. Never use inference, which would drop leading zeros and reformat decimals and datetimes.
   - Set `emptyValue` so that `""` stays the empty string, distinct from `\N` **[I: verify the Spark CSV reader distinguishes `""` from empty]**.
2. Also land `…<Table>.deleted.csv` if it exists (manifest `files` lists it).
3. **`bronze.<schema>_<table>_raw`** (append-only audit): every row plus `_drop`, `_op` (`base` | `upsert` | `delete`), `_source_file`, `_row_ordinal`.
4. **`bronze.<schema>_<table>`** (current snapshot):
   - **D0:** `CREATE OR REPLACE` from the base file. Assert the count equals `files[...].full_row_count`.
   - **D1/D2, PK table:**
     1. `DELETE WHERE pk IN deleted.csv`. Fail if a key is absent, as the capture does.
     2. `MERGE INTO snapshot USING upserts ON pk WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`.
     3. Header-only file: no-op.
     4. Assert count = `full_row_count`.
   - **D1/D2, whole_row_multiset (`*_Archive`):**
     1. For each deleted row, remove **one** matching copy. Use `row_number() over (partition by all cols)` to pick it.
     2. **Append** all upsert rows. Duplicates are allowed.
     3. Assert count = `full_row_count`.
   - Order matters only across drops. Within a drop, deletes come before upserts.
5. Drop identity: store `_loaded_drop` on the snapshot. A drop is applied **exactly once and in order**. Fail if `previous_input`'s drop is not the last applied drop.

### 4.3 Temporal history (`*_Archive`) use

- 17 OLTP tables are system-versioned, e.g. `wwi-ssdt/wwi-ssdt/Application/Tables/Cities.sql`. The in-scope ones with archive files are: Cities, Countries, StateProvinces, PaymentMethods, People, TransactionTypes, SupplierCategories, Suppliers, BuyingGroups, CustomerCategories, Customers, Colors, PackageTypes, StockItems (13 archive files per drop).
- The `Get<X>Updates` procs use them in two ways (`wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:41,64-67,83,106-109`, `GetCustomerUpdates.sql:37-72,85-112`):
  - **(a) Change detection:** scan `X_Archive ∪ X` for `ValidFrom > @LastCutoff AND ValidFrom <= @NewCutoff AND ValidFrom <> @InitialLoadDate` to build a cursor of change instants.
  - **(b) Reconstruction:** for each instant, re-read the joined row set `FOR SYSTEM_TIME AS OF @ValidFrom`.
- Databricks equivalent:
  - `silver.<x>_history = bronze.<x> UNION ALL bronze.<x>_archive`. Columns are identical, verified by manifest `columns` for Cities and Cities_Archive.
  - `as_of(t)` = `WHERE ValidFrom <= t AND ValidTo > t`. This is SQL Server's `AS OF` semantics: start inclusive, end exclusive.
  - Compare ValidFrom and ValidTo as 7-digit strings or `TIMESTAMP_NTZ` **[I: µs truncation is safe only if no two versions differ below 1 µs; strings avoid the question]**.
  - Current rows carry `ValidTo = 9999-12-31 23:59:59.9999999` **[V: D2 People.csv]**.
- Example **[V]**: D2 `People.csv` row PersonID 11 has `ValidFrom 2016-06-14 08:00:00.0000000`. D2 `People_Archive.csv` has the prior version (`2016-05-31 23:14:00` → `2016-06-14 08:00:00`). This produces the new `Dimension.Employee` row (key 213, lineage 29, `valid_from 2016-06-14 08:00:00.0000000`) in `outputs/D2/tables/Dimension.Employee.csv`.
- The source ValidFrom values are **simulated business times** (2016), not wall-clock. So `temporal.ValidFrom`/`ValidTo` in the manifest's `nondeterministic_columns` is deterministic **given these inputs**. It is nondeterministic only across recaptures. **Do not exclude** dims' `valid_from`/`valid_to` from parity.

### 4.4 Gold replay per drop

- Run the job once per drop with `(last_cutoff, new_cutoff)` taken from `manifest.drops[d]`.
- **D0 cannot be reproduced by an incremental run.** The D0 DW comes from Microsoft's backup. Its lineage rows 1-13 are dated `2016-06-06 12:36:16`… (`outputs/D0/tables/Integration.Lineage.csv`) **[V]**.
- Options for D0:
  - **(A)** Run a single full load from `2012-12-31` to `2016-06-06 12:31:17` against the D0 inputs. Lineage rows 1-13 suggest the baseline was produced by one such run: one lineage row per entity, all with cutoff `2016-06-06 12:31:17` **[V]**. Whether one run reproduces SCD2 row counts and key ordering exactly is **[I]** and is the first thing builders must prove on dim_city (116,295 rows).
  - **(B)** Seed gold from the D0 output CSVs and run only D1 and D2 incrementally. Simpler, but D0 parity then becomes trivially true.
  - Wave 0 decision: **(B)** seed gold/Integration from D0 outputs (the D0 DW is a restored baseline, not an ETL product); D1/D2 run incrementally. (A) stays optional research.

---

## 5. Migration harness: current state and per-drop requirements

### 5.1 Current state **[V]**

| File | What it does today |
|---|---|
| `golden/manifest.json` `tables` | 16 entries: 14 gold tables plus `gold.report_sale_by_day` and `gold.report_customer_history`. **Every `path` is `outputs/D2/...`.** `golden/README.md` says: "`manifest` … points the parity contract at the last available drop." **Confirmed: parity compares D2 only.** **No entries for `Integration.ETL_Cutoff` or `Integration.Lineage`**, although their CSVs exist under `outputs/D{0,1,2}/tables/`. |
| `src/parity/parity_check.py` | Hard-codes `GOLDEN_PATH`. `compare()` (`:27-62`) builds one producer per output, SKIPs unregistered ones and PASSes only on exact count plus SHA-256 match. `GoldenStore.rows` (`:105-118`) requires the path to start with `outputs/` and the header to equal `columns`, and maps `\N` to None. `actual()` (`:127-134`) runs `SELECT <cols> FROM catalog.<layer or layer_suffix>.<table>` with layer ∈ {bronze, silver, gold}. There is no drop concept and no column-level comparison or diff output. |
| `src/sql/units/registry.json` | `{}` |
| `ci/run_units.py` | `databricks bundle run -t <target> --var schema_suffix=<s> <job>` per unit in registry order. Passes **no job parameters**. |
| `ci/dev_verify.sh` | validate, deploy, classify (diagnostic only), run **all** units, parity |
| `ci/classify_changes.py:8-13` | Any path outside `src/sql/units/`, `tests/`, `README*`, `.github/` counts as "shared", which selects all units. `golden/**`, `databricks.yml`, `ci/**`, `src/parity/**` therefore all select every unit. |
| `ci/attest.py` | Fail-closed Markdown. A row is valid if its status is PASS or SKIP. |
| `ci/reset.sh`, `ci/cleanup.py` | Dev: drop marked `bronze/silver/gold_pr_<n>` schemas (CASCADE) and run `bundle destroy`. Prod: `bundle destroy` and `DROP TABLE` only for registry `outputs` (`cleanup.py:117-125`). |
| `.github/workflows/pr.yml` | Jobs `unit-tests`, then `dev` (validate, `dev_verify.sh`, attest, sticky comment, artifact) |
| `.github/workflows/deploy-prod.yml` | Push to main: validate, deploy, `run_units.py prod --target prod`, prod parity, attest |
| `.github/workflows/reset.yml`, `dev-sweep.yml` | Manual |
| `databricks.yml` | Variables `catalog`, `schema_suffix`, `bronze/silver/gold_schema`, `warehouse_id`, `golden_path`. Dev owns the PR schemas. Prod uses fixed schemas. **No `drop` variable** and no resources. |
| `tests/test_parity.py` (17 tests), `tests/test_ci.py` (13), `tests/test_capture_sqlserver.py` | Offline unit tests |

### 5.2 What per-drop parity needs

1. **Manifest and contract shape.** Add a drop dimension without editing golden files. Golden is immutable, so generate a **derived contract** in the migration repo, e.g. `src/parity/contract.json` built from `golden/manifest.json` `files`:
   ```
   {"drops": ["D0","D1","D2"],
    "tables": [{"table": "gold.fact_sale", "drop": "D1", "path": "outputs/D1/tables/Fact.Sale.csv", "columns": [...], "compare": {...}}]}
   ```
   - That is 18 outputs × 3 drops = **54 entries**: 14 tables, 2 reports and **2 Integration tables** (`gold.integration_etl_cutoff`, `gold.integration_lineage`).
   - Alternative: extend `parity_check.py` to expand `manifest.files` itself, keyed on `outputs/<drop>/`.
   - Today the README documents only `tables` as the contract. The capture's `manifest` subcommand could be re-run to emit per-drop `tables`, but that changes golden, which is not allowed. **The recommendation is the derived contract.**
2. **Where per-drop actuals live.** Gold is mutated per drop, so parity must run **after each drop**, before the next drop is applied. Alternatively, snapshot each drop: Delta `VERSION AS OF` recorded per drop, or `CREATE TABLE … SHALLOW CLONE gold_pr_<n>.fact_sale_d1`.
   - Recommendation: **parity after each drop inside the same job run**, using a parity task per drop in the Lakeflow job. Or have `dev_verify.sh` loop: run drop d, then parity d.
   - Also record the Delta version per table per drop in `out/parity.json`, so evidence is reproducible with `VERSION AS OF`.
3. **Units receive a drop parameter.**
   - Add job parameter `drop` (D0, D1, D2). Builders derive `last_cutoff` and `new_cutoff` from it, using a small shared table `bronze.golden_drops` loaded from `manifest.drops`.
   - `run_units.py` gains `--drop` and passes `--params drop=<d>` to `databricks bundle run`.
   - `databricks.yml` declares the job parameter.
   - Once the orchestrated single Lakeflow job exists, the parameter is passed once and fans out to tasks.
4. **Nondeterministic columns, without weakening strictness.**
   - `*.lineage_key`: golden keys are deterministic: 1-13 (D0), 14-26 (D1), 27-39 (D2), in `ENTITIES` order (`golden/capture_sqlserver.py:21-35`) **[V: `Integration.Lineage.csv`]**. So the shared lineage helper should **assign `lineage_key = 13*drop_index + entity_ordinal + 1`**. Then fact and dim `lineage_key` compare exactly, with no exclusion.
   - Only `Integration.Lineage.data_load_started` and `data_load_completed` are wall-clock. For those two columns only, declare a per-column comparator in the contract: `"normalize": {"data_load_started": "non_null_datetime7", "data_load_completed": "gte:data_load_started"}`. The harness replaces the value with a token (`<ts>`) on **both** sides after validating the predicate, then hashes. Every other column stays byte-exact.
   - `temporal.ValidFrom`/`ValidTo` stay exact (§4.3).
   - Unit tests must prove that a mismatched non-normalized column still FAILs, and that a NULL `data_load_completed` FAILs.
5. **Surrogate keys** (`*_key` other than lineage) are **not** declared nondeterministic, so they compare exactly. Builders must reproduce legacy insertion order for sequences and identities. That order is `INSERT … SELECT FROM *_Staging` order, i.e. staging identity order, which in turn is the `Get*Updates` result order.
   - Fact keys continue from `MAX(key)` across drops, except Stock Holding, whose `TRUNCATE` resets the identity: keys are 1..227 every drop **[V]**.
   - Re-staged facts are deleted and reinserted with new keys (Sale lineage 24 has 1,698 rows in D1 and 1,459 in D2) **[V]**.
6. **Integration ETL_Cutoff and Lineage.**
   - Add gold tables `integration_etl_cutoff(table_name STRING, cutoff_time STRING)` and `integration_lineage(lineage_key INT, data_load_started STRING, table_name STRING, data_load_completed STRING, was_successful INT, source_system_cutoff_time STRING)`. These are the golden column names (manifest `files[...Integration.Lineage.csv].columns`).
   - Register them to a platform unit (e.g. `framework_integration`).
   - The contract table layer is `gold`. They need the harness's layer allow-list (bronze|silver|gold), so do not invent an `integration` layer.
   - Golden Lineage holds 13/26/39 rows: it accumulates.
   - D0 Lineage rows 1-13 carry 2016 wall-clock times (2016-06-06 12:36…) and are fixed by the normalizer as well.
7. **Bronze landing wiring.**
   - Golden inputs are read from `/Volumes/daiwt_02_mig_prod/landing/golden/inputs/<drop>/WideWorldImporters.<Schema>.<Table>.csv`, plus optional `.deleted.csv`.
   - Pass `golden_path` (already a bundle var, `databricks.yml` variables) and `drop` to the bronze unit.
   - The dev SP needs `READ VOLUME` on `daiwt_02_mig_prod.landing.golden` **[I: Terraform grant exists per `dbx-data-platform-infra` PR #10; verify that the dev and prod SP both have it]**.
   - Bronze outputs are not in the golden contract. The registry still needs them as `outputs` so that prod reset drops them (see 8).
8. **Reset and sweep implications.**
   - `ci/cleanup.py --prod` drops only registry `outputs`. Every bronze, silver and gold table a unit creates (raw, snapshot, history, quarantine, drop-registry, per-drop clones) must be listed. Otherwise add a registry field `"owned_tables"`.
   - Bronze snapshot state is cumulative. A rerun must **reset bronze/silver/gold before D0**: `CREATE OR REPLACE` in the D0 step, and a drop-order guard (§4.2 step 5) for D1/D2.
   - Dev schemas are dropped CASCADE, so they are fine.
   - Lakeflow pipeline-managed tables (streaming tables and MVs) are dropped by `bundle destroy`, not by `DROP TABLE`. `cleanup.py --prod` must skip them or tolerate errors **[I]**.
9. **CI job-run counts.**
   - Today `dev_verify.sh` runs **all** registered units (N jobs) plus one parity pass.
   - Per-drop with separate unit jobs: N×3 job runs plus 3 parity passes.
   - With the target design (one orchestrated Lakeflow job taking a `drop` parameter): **3 job runs (D0, D1, D2) plus 3 parity passes per PR**, the same on main/prod.
   - Alternatively, use a single run with a `for_each` over drops and parity inside the job: 1 job run.
   - Mason's standing preference is to run only the changed unit's job on PRs. Under per-drop cumulative state that still needs the upstream chain, so classify should select **the unit plus its upstream closure**, not "all".
   - `classify_changes.py` currently treats `golden/**` and `src/parity/**` as shared (all units). That is acceptable. Note that it also treats `ci/**` as shared.

### 5.3 Harness gaps (this repo)

| # | File:line | Issue | Impact |
|---|---|---|---|
| H1 | `golden/manifest.json` `tables` | Points at D2 only. No Integration tables. | D0 and D1 are never compared. ETL_Cutoff and Lineage are never compared. |
| H2 | `src/parity/parity_check.py:22-25` | The digest JSON-serializes API strings vs CSV strings. Fine for strings, but a type that renders differently (BOOLEAN, TIMESTAMP, BINARY) silently FAILs with only "row count or content mismatch". | No column-level diff. Add a first-N differing rows/columns report to `out/parity.json`. |
| H3 | `src/parity/parity_check.py:22-25,105-118` | Whole golden and actual tables are loaded into memory. Sale is about 231k rows × 21 cols, and per-drop parity triples that, inside the 50 s `wait_timeout` plus chunk polling. | Feasible, but slow. Consider an in-warehouse comparison: load golden CSV via `read_files` and run `EXCEPT ALL` both ways. |
| H4 | `src/parity/parity_check.py:94-103` | `volumes.read("daiwt_02_mig_prod.landing.golden")` is hard-coded, independent of `GOLDEN_PATH`. | Fine for now. Couples dev parity to prod catalog access. |
| H5 | `src/parity/parity_check.py:138-160` | The CLI default `--suffix pr_1` silently targets PR 1's schemas if the flag is omitted. | Make the flag required. |
| H6 | `ci/run_units.py:8-15` | No job parameters (drop, cutoff). | Blocks per-drop runs. |
| H7 | `ci/dev_verify.sh:32-34` | `classify` output is not used; all units run. Parity runs once after all units. | Cannot do per-drop parity. Contradicts the "only changed unit" preference. |
| H8 | `ci/cleanup.py:117-125` | Prod reset drops only registry `outputs`. | Bronze, silver and helper tables would leak, and a rerun would replay drops onto stale cumulative state. |
| H9 | `ci/attest.py:7-14` | SKIP counts as valid, so 54/54 SKIP attests PASS. | Acceptable at baseline. After migration, add a "no SKIP for registered drops" mode or show SKIP counts in the header. |
| H11 | `golden/README.md` | States "Nondeterministic columns are recorded, not excluded", but the parity README says "no … ignored columns". | Lineage timestamps make Integration.Lineage parity impossible without the §5.2(4) normalizer, a contract change that needs sign-off. |
| H12 | `tests/` | No tests for multi-drop contracts, normalizers or the drop parameter. | Add them with the units below. |

### 5.4 Proposed platform units (file-level, no code)

| ID | Unit | Files (create or change) | Depends on |
|---|---|---|---|
| P0 | **Derived per-drop parity contract** | `src/parity/contract.py` (new: builds the 54-entry contract from `golden/manifest.json` `files`, plus a normalizer spec for Lineage timestamps), `src/parity/contract.json` (generated and committed), `tests/test_contract.py` | — |
| P1 | **Parity harness: per-drop and normalizers** | `src/parity/parity_check.py` (add `--drop`, filter the contract by drop, column normalizers, per-column diff sample, required `--suffix`), `tests/test_parity.py` | P0 |
| P2 | **Drop-parameterised runner** | `ci/run_units.py` (`--drop`, `--params`), `ci/dev_verify.sh` (loop D0→D1→D2: run, then parity, then save `out/parity-<d>.json`), `ci/attest.py` (merge 3 drop results; table rows × drop), `.github/workflows/pr.yml` and `deploy-prod.yml` (upload 3 parity files), `tests/test_ci.py` | P1 |
| P3 | **Shared framework SQL** | `src/sql/common/constants.sql` (END/START_OF_TIME), `common/lineage.sql` (deterministic keys), `common/cutoff.sql`, `common/scd2.sql`, `common/dim_lookup.sql`, `common/surrogate_keys.sql`, `common/formats.sql` (dt7 and bit helpers), `common/as_of.sql`; `databricks.yml` job params `drop`, `golden_path` | — |
| P4 | **Bronze golden landing** (25 tables plus 13 archives) | `src/sql/units/bronze_wwi.sql` (or a Python notebook), the bundle job resource, a `registry.json` entry (outputs = all bronze tables), `bronze.golden_drops` loaded from `manifest.drops`, `tests/test_bronze_replay.py` (offline replay against the repo golden: counts equal `full_row_count` for D1/D2, multiset semantics, deleted.csv path tested with a synthetic fixture) | P3 |
| P5 | **Framework gold: Integration tables and unknown members** | `src/sql/units/framework_integration.sql` (`gold.integration_etl_cutoff`, `gold.integration_lineage`, unknown-member seed into 7 dims, `dim_date` generator using the live 14-column logic), registry | P3, P4 |
| P6 | **Silver temporal history views** (`<x>_history`, `as_of`) | `src/sql/units/silver_history.sql` | P4 |
| P7 | **Registry and cleanup ownership** | `src/sql/units/registry.json` schema (`owned_tables`, `depends_on`), `ci/cleanup.py` (drop owned tables, tolerate pipeline-managed tables), `ci/classify_changes.py` (unit plus upstream closure), tests | P4, P5 |
| P8 | **Orchestrated Lakeflow job** (one job, `drop` param, tasks bronze → silver → gold → reports → parity-per-drop optional) | `resources/wwi_daily_job.yml`, `databricks.yml` include | P2–P7 and entity units |
| P9 | **README contract update** | `README.md` "Golden contract" (per-drop, normalizer rule, type rules from §3) | P0, P1 |

Entity workstreams (dims, facts, reports) build on P3–P6. Each registers its outputs and receives `drop`.
