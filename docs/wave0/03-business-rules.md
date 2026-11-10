# WWI business rules & stored procedures — Wave 0 workstream (c)

Repos read (read-only): legacy `daiwt-02-enterprise-etl` (= **L/**), migration `daiwt-02-lakehouse-migration@main` (= **M/**). Golden = `M/golden/`.
Legacy commit captured: `bb42a64450b60185ac7b9bab5863cbac43449c61` (`M/golden/capture-D2.json` → `source.legacy_commit`). Source engine: SQL Server 2022 RTM-CU22 Express.
Legend: **[V]** verified against golden CSVs (script recomputation) · **[C]** read from code · **[I]** inference.

---
## 1. The exact code path that produced the goldens

| Item | Fact | Evidence |
|---|---|---|
| Orchestrator | Not an SSIS run. `capture_sqlserver.py etl` replays the native `DailyETLMain.dtsx` sequence in Python. | `M/golden/capture_sqlserver.py:661-707` |
| Order of entities | City, Customer, Employee, Payment Method, Stock Item, Supplier, Transaction Type, Movement, Order, Purchase, Sale, Stock Holding, Transaction | `capture_sqlserver.py` `ENTITIES` (top of file) |
| Per entity | `EXEC Integration.GetLineageKey(table, cutoff)` → clear staging (`CLEAR_SQL`) → `GetLastETLCutoffTime` → `INSERT INTO DW.Integration.<Key>_Staging (cols) EXEC OLTP.Integration.Get<Key>Updates @LastCutoff, @NewCutoff` → `EXEC Integration.MigrateStaged<Key>Data` | `capture_sqlserver.py:677-689` |
| Before the loop | `EXEC Integration.PopulateDateDimensionForYear <cutoff.year>` (2016 — already present, so no-op) | `capture_sqlserver.py:673` |
| Stock Holding | called with **no** cutoff args (full snapshot) | `capture_sqlserver.py:682` |
| `WITH RESULT SETS` | Used **only** to describe columns (`sys.dm_exec_describe_first_result_set`) and to map names → staging columns; the actual `INSERT…EXEC` does **not** include `WITH RESULT SETS`, so result-set casts are **not applied**; insert is positional. | `capture_sqlserver.py:640-656, 683-685`; `M/golden/native_result_sets.json` |
| Name mapping | `Movement: Last Modified When→Last Modifed When`, `Purchase: Is Order Line Finalized→Is Order Finalized` | `M/golden/native_destination_columns.json` |
| Staging clear | `DELETE FROM Integration.<Key>_Staging` except **Purchase → `DELETE FROM Integration.Order_Staging`** (matches native package) | `capture_sqlserver.py:36-39`; `L/wwi-ssis/wwi-ssis/DailyETLMain.dtsx:5061` |
| Procs that run | OLTP: the 13 `Integration.Get*Updates` in `L/wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/`. DW: `GetLineageKey`, `GetLastETLCutoffTime`, 13 `MigrateStaged*Data`, `PopulateDateDimensionForYear` in `L/wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/`. | file list; result-set shapes in `native_result_sets.json` match these procs (with renames below) |
| Not run | Everything under `L/sqlserver/` (`Integration.usp_LoadFactSale`, `usp_DeduplicateFactSale`, `usp_ApplyFactCorrections`, aggregates, `Report.vw_*`, `etl.*`/`stg.*` procs) and all 205 packages in `ssis_branches.json` (except the replayed DailyETLMain sequence). `manifest.json:297` `skipped_branches.reason`: "Missing live procedures in both databases". | `M/golden/ssis_branches.json`, `M/golden/manifest.json:297` |
| D0 outputs | Pre-existing DW state (lineage keys 1-13, cutoff `2016-06-06 12:31:17`), not produced by the capture ETL. D1 = lineage 14-26 (cutoff 2016-06-14), D2 = 27-39 (cutoff 2016-06-21). | `outputs/D2/tables/Integration.Lineage.csv` **[V]** |
| Date cutoff | `Integration.ETL_Cutoff` row `Date` stays `2012-12-31` (never advanced). | `outputs/D1/tables/Integration.ETL_Cutoff.csv` **[V]** |

Repo code that disagrees with golden:
* `L/wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql` (~60 columns incl. `DateKey INT`) and `PopulateDateDimensionForYear.sql:19-60` (inserts `DateKey`, `Day of Year`, …) **do not match** golden `Dimension.Date.csv` (14 columns: `date, day_number, day, month, short_month, calendar_month_number, calendar_month_label, calendar_year, calendar_year_label, fiscal_month_number, fiscal_month_label, fiscal_year, fiscal_year_label, iso_week_number`; 1,461 rows 2013-01-01..2016-12-31). **[V]** The deployed live Date table/proc is the original Microsoft shape; the repo files are a variant. Treat golden `dim_date` as a static seed. **[I]**
* `GetMovementUpdates.sql:20` returns `Transaction Occurred When` as column 10; `native_result_sets.json` names it `Last Modified When`; it lands positionally in `Movement_Staging.[Last Modifed When]` (`Integration/Tables/Movement_Staging.sql:16`). So Movement key lookups use **TransactionOccurredWhen**. **[C]**
* `GetSaleUpdates.sql:28` aliases `[WWI Saleperson ID]`; staging/result set use `[WWI Salesperson ID]` (positional insert makes it harmless). **[C]**
* `native_result_sets.json` StockHolding declares `[Last Cost Price] int`, but staging/fact are `DECIMAL(18,2)` (`Fact/Tables/Stock Holding.sql:7`) and golden has 114/227 non-integer values (e.g. `9.50`) → result-set cast not applied. **Golden = decimal(18,2).** **[V]**

---
## 2. Extraction procs (OLTP `Integration.Get*Updates`, `L/wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/`)

### 2.1 Dimension extracts (temporal, cursor-driven)
Common pattern **[C]**: for each driving table, cursor over `ValidFrom` in `(@LastCutoff, @NewCutoff]` from `<table>_Archive UNION ALL <table>`, ordered by `ValidFrom`; for each change re-read the whole entity `FOR SYSTEM_TIME AS OF @ValidFrom`; collect into `#…Changes`; then `Valid To = COALESCE(MIN(later Valid From for same ID), @EndOfTime)` where `@EndOfTime = '9999-12-31 23:59:59.9999999'`; output `ORDER BY [Valid From]`. No dedup: the same (ID, ValidFrom) can appear twice if two driving tables change at the same instant **[I]**.

| Proc | Driving changes (lines) | Joins (all `FOR SYSTEM_TIME AS OF @ValidFrom`) | Defaults / expressions | Valid From |
|---|---|---|---|---|
| GetCityUpdates | Countries (42-49), StateProvinces (84-91), Cities (126-132) | Cities ⋈ StateProvinces ⋈ Countries (INNER) 64-69 | `COALESCE(LatestRecordedPopulation,0)` (63,105,145) | `@ValidFrom` (driver time), `Valid To` NULL then computed 166-169 |
| GetCustomerUpdates | BuyingGroups (36-48, excl. `@InitialLoadDate`), CustomerCategories (84-96, excl. initial), Customers (132-143, **incl.** initial) | Customers c ⋈ CustomerCategories (INNER) ⋈ Customers bt on BillToCustomerID (INNER) ⋈ People p on PrimaryContactPersonID (INNER) ⟕ BuyingGroups (LEFT) 157-166 | `Bill To Customer = bt.CustomerName`, `Primary Contact = p.FullName`, `Postal Code = c.DeliveryPostalCode`; Buying Group may be NULL (no COALESCE) | `c.ValidFrom` |
| GetEmployeeUpdates | People (34-41) | People only, `WHERE p.PersonID=@PersonID` (56-57) | `[Is Salesperson]=p.IsSalesperson`, Photo raw. No `IsEmployee` filter in repo | `p.ValidFrom` |
| GetPaymentMethodUpdates | PaymentMethods (31-37) | — | — | `p.ValidFrom` |
| GetStockItemUpdates | StockItems (45-51) | StockItems ⋈ PackageTypes spt (selling) ⋈ PackageTypes bpt (buying) (INNER) ⟕ Colors (LEFT) 69-76 | `ISNULL(Color/Brand/Size/Barcode, N'N/A')` (98-103) | `si.ValidFrom` |
| GetSupplierUpdates | SupplierCategories (38-45), Suppliers (80-86) | Suppliers ⋈ SupplierCategories ⋈ People on PrimaryContactPersonID (INNER) 60-65, 100-105 | `Postal Code = s.DeliveryPostalCode` | `s.ValidFrom` |
| GetTransactionTypeUpdates | TransactionTypes (31-37) | — | — | `p.ValidFrom` |

Golden checks **[V]**: Dimension City/Customer/Stock Item/Supplier/Payment Method/Transaction Type are byte-identical D0→D2 (no changes in windows). Employee D2 adds key 213 (WWI 11 "Ethan Onslow", `valid_from 2016-06-14 08:00:00`, lineage 29) and closes key 203 `valid_to = 2016-06-14 08:00:00.0000000`. `N/A` counts in D2 dim_stock_item: barcode 656, brand 605, color 312, size 204 (of 672). dim_city `latest_recorded_population='0'`: 34,275 rows. Each dim has key-0 "Unknown" row (`valid_from 2013-01-01`, `valid_to 9999-12-31 23:59:59.9999999`, lineage 0, text cols `N/A`, numerics 0, location `\N`).

### 2.2 Fact extracts (`LastEditedWhen` window, set-based)
Window for all except Stock Holding: `X > @LastCutoff AND X <= @NewCutoff`, where X = `GREATEST(line.LastEditedWhen, header.LastEditedWhen)` via `CASE WHEN line > header THEN line ELSE header END` (Order/Purchase/Sale) or single-table `LastEditedWhen` (Movement/Transaction). **[C]**

| Proc (lines) | Source/join | Column rules |
|---|---|---|
| GetMovementUpdates (9-24) | `Warehouse.StockItemTransactions` only | `Date Key = CAST(TransactionOccurredWhen AS date)`; `Quantity = CAST(Quantity AS int)` (source decimal(18,3) → truncation toward 0); 10th col = `TransactionOccurredWhen` (→ lookup timestamp) |
| GetOrderUpdates (12-39) | Orders ⋈ OrderLines ⋈ PackageTypes ⋈ Customers (INNER) | `Order Date Key=CAST(OrderDate AS date)`, `Picked Date Key=CAST(ol.PickingCompletedWhen AS date)` (NULL if not picked); `Total Excluding Tax = ROUND(Q*UnitPrice,2)`; `Tax Amount = ROUND(Q*UnitPrice*TaxRate/100.0,2)`; `Total Including Tax` = sum of the two rounded values; `WWI City ID = c.DeliveryCityID` (customer's **current** delivery city, not temporal); `WWI Picker ID = o.PickedByPersonID` |
| GetPurchaseUpdates (12-31) | PurchaseOrders ⋈ PurchaseOrderLines ⋈ StockItems ⋈ PackageTypes | `Date Key=CAST(po.OrderDate AS date)`; `Ordered Quantity = OrderedOuters * si.QuantityPerOuter` (int*int, current stock item); `Is Order Finalized = pol.IsOrderLineFinalized` (line-level flag despite name) |
| GetSaleUpdates (11-44) | Invoices ⋈ InvoiceLines ⋈ StockItems ⋈ PackageTypes ⋈ Customers c ⋈ Customers bt (INNER) | `Invoice Date Key=CAST(InvoiceDate AS date)`; `Delivery Date Key=CAST(ConfirmedDeliveryTime AS date)`; `Total Excluding Tax = ExtendedPrice - TaxAmount`; `Tax Amount=TaxAmount`; `Profit=LineProfit`; `Total Including Tax=ExtendedPrice`; `Total Dry Items = CASE WHEN IsChillerStock=0 THEN Quantity ELSE 0`; `Total Chiller Items = CASE WHEN IsChillerStock<>0 THEN Quantity ELSE 0`; `WWI City ID = c.DeliveryCityID`; order `InvoiceID, InvoiceLineID` |
| GetStockHoldingUpdates (9-17) | `Warehouse.StockItemHoldings` (all rows, no window) | straight copy, ordered by StockItemID |
| GetTransactionUpdates (11-56) | CustomerTransactions ⟕ Invoices `UNION ALL` SupplierTransactions | Customer branch: `WWI Customer ID = COALESCE(i.CustomerID, ct.CustomerID)`, `WWI Bill To Customer ID = ct.CustomerID`, supplier cols NULL. Supplier branch: customer/invoice cols NULL. Amounts copied as-is; `Date Key = CAST(TransactionDate AS date)` |

---
## 3. Load procs (DW `Integration.MigrateStaged*Data`, `L/wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/`)

### 3.1 Dimensions (City, Customer, Employee, Payment Method, Stock Item, Supplier, Transaction Type) — identical pattern
1. `@LineageKey = TOP(1) [Lineage Key] … WHERE [Table Name]=<name> ORDER BY [Lineage Key] DESC` (lines 13-17).
2. Close-off: `WITH RowsToCloseOff AS (SELECT id, MIN([Valid From]) FROM staging GROUP BY id) UPDATE dim SET [Valid To]=MIN(staged Valid From) WHERE [Valid To]=@EndOfTime` (22-31).
3. `INSERT` all staging rows as-is with `@LineageKey` (33-41). No dedupe, no change detection (a no-op change still creates a new version).
4. `Integration.Lineage`: `Data Load Completed=SYSUTCDATETIME(), Was Successful=1`; `ETL Cutoff.[Cutoff Time] = Lineage.[Source System Cutoff Time]` (44-54).
Surrogate key = IDENTITY (next value). **[V]** Employee D2 example above.

### 3.2 Facts — key lookup rule (Movement, Order, Purchase, Sale, Transaction)
```
key = COALESCE((SELECT TOP(1) d.key FROM dim d
                WHERE d.natural_id = stg.natural_id
                  AND stg.[Last Modified When] >  d.[Valid From]
                  AND stg.[Last Modified When] <= d.[Valid To]
                ORDER BY d.[Valid From]), 0)
```
Half-open window `(Valid From, Valid To]` — **exclusive lower, inclusive upper** (opposite of the usual `[from,to)`). NULL natural id → 0. Lookup timestamp: Sale/Order/Purchase = GREATEST(line,header LastEditedWhen); Transaction = ct/st.LastEditedWhen; Movement = TransactionOccurredWhen. Lines: Movement 19-43, Order 19-49, Purchase 19-31, Sale 19-49, Transaction 19-49.

| Fact | Lookups | Apply changes | Golden evidence **[V]** |
|---|---|---|---|
| Movement | stock item, customer, supplier, transaction type | `MERGE` on `[WWI Stock Item Transaction ID]`: matched → update all cols + lineage; not matched → insert (48-65). No delete. | D2: lineage 8=236,667, 21=1,462, 34=1,854 rows |
| Order | city, customer, stock item, salesperson, picker | `DELETE Fact.Order WHERE [WWI Order ID] IN staging` then `INSERT` all staged lines (54-69) | D2: 9=231,370, 22=1,445, 35=1,852; 3,297 D1+D2 rows recomputed, 0 arithmetic mismatches |
| Purchase | supplier, stock item | `DELETE WHERE [WWI Purchase Order ID] IN Purchase_Staging` then `INSERT` (36-47) | see §1 staging-clear rule; D2: 10=8,358, 23=0, 36=113 |
| Sale | city, customer, bill-to, stock item, salesperson | `DELETE WHERE [WWI Invoice ID] IN staging` then `INSERT` (54-67) | 3,505 D1+D2 rows recomputed from inputs: 0 mismatches (totals, tax, profit, dry/chiller, unit price, qty) |
| Stock Holding | stock item = `TOP(1) … ORDER BY [Valid To] DESC` (latest version, no time window), else 0 (19-23) | `TRUNCATE` + `INSERT` (28-37) | D2: 227 rows all lineage 38; StockItemID 1 → key 603 |
| Transaction | customer, bill-to, supplier, transaction type, payment method | **Insert only** (54-63); no delete/merge | D2: 13=99,585, 26=705, 39=842 |

Source arithmetic **[V]**: in all 231,486 reconstructed `Sales.InvoiceLines`, `TaxAmount = ROUND_HALF_UP(Q*UP*TR/100, 2)` and `ExtendedPrice = Q*UP + TaxAmount` hold for every row. Example: Fact.Sale key 229964 (invoice 70866): Q=120, UP=18.00, TR=15.000 → excl 2160.00, tax 324.00, incl 2484.00, profit 1320.00, dry 120; invoice date 2016-06-13, delivery 2016-06-14 (ConfirmedDeliveryTime 2016-06-14 07:05).

Key-0 prevalence in golden D2 (rows by lineage) **[V]**:
| Fact | customer_key=0 | other key=0 |
|---|---|---|
| Sale (11/24/37) | 84,170 / 535 / 762 (bill_to identical) | city, stock_item, salesperson: 0 |
| Order (9/22/35) | 85,449 / 506 / 688 | picker 23,698 / 152 / 174 |
| Movement (8/21/34) | 92,699 / 541 / 724 | supplier 228,309 / 1,414 / 1,807 |
| Transaction (13/26/39) | 52,968 / 368 / 421 | supplier 97,147/689/828; payment_method 70,510/519/639 |
Golden rule: a fact row gets customer_key 0 when its WWI customer id has no `Dimension.Customer` row in the lookup window. Golden `dim_customer` is 403 rows (incl. key 0) in every drop; all 762 D2-new Sale rows with customer_key 0 follow this rule. Build `dim_customer` to reproduce the golden row set, not the full OLTP customer list.

---
## 4. The 2 reports (exact SQL: `M/golden/capture_sqlserver.py:40-50`, executed by `:502-515`)

| Report | SQL | Semantics |
|---|---|---|
| sale-by-day | `SELECT [Invoice Date Key], COUNT_BIG(*) AS SaleRows, SUM([Total Including Tax]) AS TotalIncludingTax FROM Fact.Sale GROUP BY [Invoice Date Key] ORDER BY [Invoice Date Key]` | No filter (includes key-0 customer rows, credit notes, all lineages). SUM of decimal(18,2) → decimal(38,2), no rounding. Only dates with sales appear (1,081 rows in D2; no zero-fill). `[Invoice Date Key]` NOT NULL. **[V]** recomputed from D2 Fact.Sale: 1,081/1,081 match. |
| customer-history | `SELECT [Customer Key],[WWI Customer ID],Customer,[Valid From],[Valid To] FROM Dimension.Customer ORDER BY [Customer Key]` | No filter; includes key 0 'Unknown'. 403 rows D0=D1=D2. Values equal to dim_customer projection **[V]**. |

Wrapper (`:507-510`): `SELECT CONVERT(...) AS col … FROM (<body>) AS report ORDER BY <order>` — every column is formatted with `format_sql` and the ORDER BY resolves to the **formatted string alias**, so file order is string order: customer-history order is `0,1,10,100,101,…` (lexicographic), not numeric **[V]**. sale-by-day `yyyy-mm-dd` sorts identically either way. Headers are snake_case (`invoice_date_key,sale_rows,total_including_tax`; `customer_key,wwi_customer_id,customer,valid_from,valid_to`). No legacy view/proc in `L/sqlserver/views/` or Power BI produces these exact outputs (closest, unused: `Report.vw_DailySalesTrend.sql`) **[C]**.

---
## 5. Golden CSV value formatting (`capture_sqlserver.py:315-334`)
| SQL type | Golden text | Spark equivalent |
|---|---|---|
| date | `CONVERT(varchar(10),x,23)` → `2016-06-13` | `date_format(x,'yyyy-MM-dd')` |
| datetime2(7) | style 121 → `2016-06-14 08:00:00.0000000` (always 7 fractional digits) | Spark timestamps are µs: format `yyyy-MM-dd HH:mm:ss.SSSSSS` + `'0'`; `9999-12-31 23:59:59.9999999` is not representable → keep end-of-time as string/sentinel |
| decimal/money | `CONVERT(varchar(50),x)` → full declared scale (`15.000`, `0.00`) | cast to `DECIMAL(p,s)` then `CAST AS STRING` (keeps scale); never via double |
| bit | `0`/`1` | cast boolean → int → string |
| geography | `.STAsText()` → `POINT (-78.651695 42.1083969)` (lon lat, shortest round-trip digits) | store WKT string from bronze as-is; do not re-render from doubles |
| varbinary (Photo) | `CONVERT(varchar(max),x,2)` hex uppercase, no `0x` | `upper(hex(x))` |
| float | style 3 (17 sig digits) | n/a in scope |
| NULL | `\N` | `nullValue='\\N'` |

---
## 6. Spark SQL translation notes (per rule)
| Rule | T-SQL behaviour | Spark/Databricks action |
|---|---|---|
| Order `ROUND(Q*UP,2)`, `ROUND(Q*UP*TR/100.0,2)` | Q int × UP decimal(18,2) × TR decimal(18,3) / 100.0 → exact decimal; ROUND half away from zero | Use `DECIMAL` arithmetic (ANSI mode), `round()` (HALF_UP) — not `bround`; never double. Sum the two rounded parts for incl. tax. |
| Sale amounts | Copied from source decimals; `ExtendedPrice - TaxAmount` exact | decimal subtraction; keep scale 2 |
| Purchase `OrderedOuters * QuantityPerOuter` | int*int → int (overflow errors) | `INT` multiply; ANSI mode errors on overflow similarly |
| Movement `CAST(Quantity AS int)` | decimal → int truncates toward zero | `CAST(q AS INT)` truncates too (ANSI: errors on overflow) |
| Integer division | none in scope procs (`/100.0` forces decimal) | ensure `/ 100.0` not `/ 100` — Spark `/` always returns decimal/double; use `CAST` to decimal to keep exactness |
| `CAST(datetime AS date)` | truncation | `to_date()` / `CAST AS DATE` (watch session timezone: source values are naive local → use `TIMESTAMP_NTZ`) |
| `CASE WHEN a>b THEN a ELSE b` | NULL-safe-ish: if a NULL → b | `greatest(a,b)` ignores NULLs (same result unless both NULL). Columns are NOT NULL in source. |
| Dim lookup `(Valid From, Valid To]` + `TOP(1) ORDER BY Valid From` + `COALESCE(…,0)` | correlated subquery | LEFT JOIN on natural id AND `ts > vf AND ts <= vt`, `row_number() OVER (PARTITION BY fact row ORDER BY vf)`=1, `coalesce(key,0)` |
| SCD Valid To | `COALESCE(MIN(later vf), EndOfTime)`; close previous current row with MIN staged vf | `lead(valid_from) OVER (PARTITION BY id ORDER BY valid_from)` + `coalesce`, then MERGE to close `valid_to = end_of_time` rows |
| Surrogate keys | IDENTITY (gaps possible, continue from max) | Manifest nondeterministic list is only `*.lineage_key`, Lineage load timestamps, `temporal.ValidFrom/ValidTo` (`manifest.json`) — surrogate keys ARE compared. Seed from golden D0 and assign `max+row_number()` ordered as SQL inserted (staging order `Valid From`, then sale/order source order) |
| Order/Sale/Purchase replace | DELETE by business id then INSERT | Delta `MERGE … WHEN NOT MATCHED BY SOURCE` not applicable; do `DELETE WHERE id IN (staged)` + `INSERT`, or `replaceWhere` |
| Movement MERGE | upsert on WWI Stock Item Transaction ID | Delta `MERGE` |
| Stock Holding | TRUNCATE+INSERT | `INSERT OVERWRITE` |
| Transaction | INSERT only (re-edited source rows append) | `INSERT INTO` / append — do **not** MERGE |
| `ISNULL(x,N'N/A')`, `COALESCE(pop,0)` | | `coalesce` — note empty string `''` is not NULL in either |
| String compare / joins | SSDT `<DefaultCollation>Latin1_General_100_CI_AS` (`L/wwi-ssdt`, `L/wwi-dw-ssdt` .sqlproj) — live collation not checked (case-insensitive, trailing spaces ignored in `=`) | In-scope joins are all on int IDs → no collation impact. Any string grouping/joins: use `UTF8_LCASE`/`lower(rtrim())`. |
| NULL concatenation | `+` with NULL → NULL (CONCAT_NULL_YIELDS_NULL ON); only used in Date dim labels | `concat()` also yields NULL; `concat_ws` does not |
| DATEPART / ISO week | `DATEPART(ISO_WEEK,…)`; `DATEPART(weekday)` depends on `@@DATEFIRST` (US=7 → Sunday=1) | `weekofyear()` = ISO week ✓; `dayofweek()` Sunday=1 ✓ (matches DATEFIRST 7 only) |
| bit | 0/1, `<>0` tests | boolean; emit `0/1` strings for parity |
| Geography | stored as CLR type | keep WKT text from bronze `Location` (already STAsText in golden inputs) |
| datetime2(7) | 100 ns precision | Spark µs: last digit is always `0` in golden — verify no non-zero 7th digit before relying (all sampled values end `0000000`) **[V sample]** |

---
## 7. Builder task seeds
1. Seed gold dims/facts from **golden D0 outputs** (they are the restored DW, not reproducible from D0 inputs); apply D1, D2 increments with the rules above. `dim_date` = static copy of golden.
2. Incremental window per entity: `(last_cutoff, new_cutoff]` on the GREATEST(header,line) LastEditedWhen; cutoffs from `capture-D*.json → drop.last_cutoff/new_cutoff` (D1 2016-06-14 00:00, D2 2016-06-21 00:00).
3. Reproduce the staging-clear behaviour of §1 for Purchase (golden depends on it).
4. Reports: implement exact SQL in §4; format per §5; order customer-history lexicographically on the string key if file order matters to parity (`M/src/parity/parity_check.py:22-23` digests sorted JSON rows → row order is irrelevant to parity **[C]**).
