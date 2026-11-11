# daiwt-02 estate map

Legacy: [daiwt-02-enterprise-etl](https://github.com/Cognition-Partner-Workshops-mirror/daiwt-02-enterprise-etl) at `bb42a64`. SQL Server 2022 `WideWorldImporters` (OLTP) feeds SSIS `DailyETLMain.dtsx`, which loads `WideWorldImportersDW`. Drops: D0 only. The plan is in [`docs/units.yml`](units.yml).

## Waves

- **Silver wave (9 units, one builder per target, all in parallel):** the 7 SCD2 dimensions, the date dimension and the ETL control tables. None of them depends on another unit; they read bronze only. Their targets are `silver.dim_*` / `silver.integration_*`, bound to the `Dimension.*` / `Integration.*` goldens through each output's `golden` path.
- **Gold wave (8 units, one builder per target, all in parallel, after Silver merges):** the 6 facts, which read `silver.dim_*` for surrogate-key lookups, and the 2 reports. `report_sale_by_day` reads `gold.fact_sale`; offline it resolves to the Sale golden, so it can be built in the same wave.
- The legacy `Integration.*_Staging` tables have no golden, so they stay inside their unit.

```mermaid
flowchart LR
  subgraph Silver
    load_city_dimension["load_city_dimension<br/>silver.dim_city"]
    load_customer_dimension["load_customer_dimension<br/>silver.dim_customer"]
    load_employee_dimension["load_employee_dimension<br/>silver.dim_employee"]
    load_payment_method_dimension["load_payment_method_dimension<br/>silver.dim_payment_method"]
    load_stock_item_dimension["load_stock_item_dimension<br/>silver.dim_stock_item"]
    load_supplier_dimension["load_supplier_dimension<br/>silver.dim_supplier"]
    load_transaction_type_dimension["load_transaction_type_dimension<br/>silver.dim_transaction_type"]
    ensure_date_dimension["ensure_date_dimension<br/>silver.dim_date"]
    etl_control["etl_control<br/>silver.integration_lineage, silver.integration_etl_cutoff"]
  end
  subgraph Gold
    load_movement_fact["load_movement_fact<br/>gold.fact_movement"]
    load_order_fact["load_order_fact<br/>gold.fact_order"]
    load_purchase_fact["load_purchase_fact<br/>gold.fact_purchase"]
    load_sale_fact["load_sale_fact<br/>gold.fact_sale"]
    load_stock_holding_fact["load_stock_holding_fact<br/>gold.fact_stock_holding"]
    load_transaction_fact["load_transaction_fact<br/>gold.fact_transaction"]
    report_sale_by_day["report_sale_by_day<br/>gold.report_sale_by_day"]
    report_customer_history["report_customer_history<br/>gold.report_customer_history"]
  end
  load_stock_item_dimension --> load_movement_fact
  load_customer_dimension --> load_movement_fact
  load_supplier_dimension --> load_movement_fact
  load_transaction_type_dimension --> load_movement_fact
  load_city_dimension --> load_order_fact
  load_customer_dimension --> load_order_fact
  load_stock_item_dimension --> load_order_fact
  load_employee_dimension --> load_order_fact
  load_supplier_dimension --> load_purchase_fact
  load_stock_item_dimension --> load_purchase_fact
  load_city_dimension --> load_sale_fact
  load_customer_dimension --> load_sale_fact
  load_stock_item_dimension --> load_sale_fact
  load_employee_dimension --> load_sale_fact
  load_stock_item_dimension --> load_stock_holding_fact
  load_customer_dimension --> load_transaction_fact
  load_supplier_dimension --> load_transaction_fact
  load_transaction_type_dimension --> load_transaction_fact
  load_payment_method_dimension --> load_transaction_fact
  load_sale_fact --> report_sale_by_day
  load_customer_dimension --> report_customer_history
```

| Legacy job (DailyETLMain.dtsx container / source) | Wave | Unit | Target tables | Golden files (golden_path) |
|---|---|---|---|---|
| Load City Dimension (`GetCityUpdates.sql`, `MigrateStagedCityData.sql`) | Silver | `load_city_dimension` | `silver.dim_city` | `outputs/D0/tables/Dimension.City.csv` |
| Load Customer Dimension (`GetCustomerUpdates.sql`, `MigrateStagedCustomerData.sql`) | Silver | `load_customer_dimension` | `silver.dim_customer` | `outputs/D0/tables/Dimension.Customer.csv` |
| Load Employee Dimension (`GetEmployeeUpdates.sql`, `MigrateStagedEmployeeData.sql`) | Silver | `load_employee_dimension` | `silver.dim_employee` | `outputs/D0/tables/Dimension.Employee.csv` |
| Load Payment Method Dimension (`GetPaymentMethodUpdates.sql`, `MigrateStagedPaymentMethodData.sql`) | Silver | `load_payment_method_dimension` | `silver.dim_payment_method` | `outputs/D0/tables/Dimension.Payment_Method.csv` |
| Load Stock Item Dimension (`GetStockItemUpdates.sql`, `MigrateStagedStockItemData.sql`) | Silver | `load_stock_item_dimension` | `silver.dim_stock_item` | `outputs/D0/tables/Dimension.Stock_Item.csv` |
| Load Supplier Dimension (`GetSupplierUpdates.sql`, `MigrateStagedSupplierData.sql`) | Silver | `load_supplier_dimension` | `silver.dim_supplier` | `outputs/D0/tables/Dimension.Supplier.csv` |
| Load Transaction Type Dimension (`GetTransactionTypeUpdates.sql`, `MigrateStagedTransactionTypeData.sql`) | Silver | `load_transaction_type_dimension` | `silver.dim_transaction_type` | `outputs/D0/tables/Dimension.Transaction_Type.csv` |
| Ensure Date Dimension includes current year | Silver | `ensure_date_dimension` | `silver.dim_date` | `outputs/D0/tables/Dimension.Date.csv` |
| Calculate ETL Cutoff Time + Get Lineage Key / Update cutoff (all containers) | Silver | `etl_control` | `silver.integration_lineage`, `silver.integration_etl_cutoff` | `outputs/D0/tables/Integration.Lineage.csv`, `outputs/D0/tables/Integration.ETL_Cutoff.csv` |
| Load Movement Fact (`GetMovementUpdates.sql`, `MigrateStagedMovementData.sql`) | Gold | `load_movement_fact` | `gold.fact_movement` | `outputs/D0/tables/Fact.Movement.csv` |
| Load Order Fact (`GetOrderUpdates.sql`, `MigrateStagedOrderData.sql`) | Gold | `load_order_fact` | `gold.fact_order` | `outputs/D0/tables/Fact.Order.csv` |
| Load Purchase Fact (`GetPurchaseUpdates.sql`, `MigrateStagedPurchaseData.sql`) | Gold | `load_purchase_fact` | `gold.fact_purchase` | `outputs/D0/tables/Fact.Purchase.csv` |
| Load Sale Fact (`GetSaleUpdates.sql`, `MigrateStagedSaleData.sql`) | Gold | `load_sale_fact` | `gold.fact_sale` | `outputs/D0/tables/Fact.Sale.csv` |
| Load Stock Holding Fact (`GetStockHoldingUpdates.sql`, `MigrateStagedStockHoldingData.sql`) | Gold | `load_stock_holding_fact` | `gold.fact_stock_holding` | `outputs/D0/tables/Fact.Stock_Holding.csv` |
| Load Transaction Fact (`GetTransactionUpdates.sql`, `MigrateStagedTransactionData.sql`) | Gold | `load_transaction_fact` | `gold.fact_transaction` | `outputs/D0/tables/Fact.Transaction.csv` |
| Capture report `sale-by-day` (deployment/rds/estate.py) | Gold | `report_sale_by_day` | `gold.report_sale_by_day` | `outputs/D0/reports/sale-by-day.csv` |
| Capture report `customer-history` (deployment/rds/estate.py) | Gold | `report_customer_history` | `gold.report_customer_history` | `outputs/D0/reports/customer-history.csv` |

## Builder notes

- Golden D0 is the restored DW at source cutoff `2016-06-06 12:31:17` (one load per entity, lineage keys 1-13). By default, rebuild from D0 bronze as one window `(2012-12-31, 2016-06-06 12:31:17]`, using base + `_archive` bronze tables as the temporal history. Where a unit note says the golden differs from that replay (e.g. Customer), the golden wins.
- Surrogate keys (`*_key`) and `lineage_key` are compared strictly. Every dimension golden includes a key 0 "Unknown" row.
- Fact dimension lookups match `ts > valid_from AND ts <= valid_to` and fall back to key 0. Stock Holding uses the latest version instead.
- Golden timestamps carry 7 fractional digits (`datetime2(7)`; end-of-time is `9999-12-31 23:59:59.9999999`).

## Out of scope (no golden output)

- `sqlserver/` expanded batch (`stg`/`etl`/report views, aggregate and extra marts), the `ssis/` packages other than `DailyETLMain.dtsx`, `oracle/`, SQL Agent job scripts, `workload-drivers/`, the app/Power BI/SSAS folders, and OLTP-only procedures (`Website.*`, `DataLoadSimulation.*`, `Sequences.*`).
- Unit-internal only (no golden): all `Integration.*_Staging` tables, and the procedure branches the capture recorded as skipped in `golden/manifest.json`.
