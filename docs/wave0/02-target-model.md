# WWI target data model & rationalization — Wave 0(b)

## §0 Executive summary and scope

[Verified] The capture enumerates **13 extraction entities** (seven versioned dimensions and six facts); Date is separate, yielding eight dimension outputs, six facts, two reports and two Integration exports. `golden/capture_sqlserver.py:21-49`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Target **38 bronze tables → 13 conformed silver entities plus silver.date → 16 business gold outputs**, with two platform control tables outside gold. This is a design proposal based on the consumed D0 inventory and registered output contract, not an implemented deployment. `golden/manifest.json:1296-1324`; golden profile (`golden/outputs/<drop>/`)

[Verified] Discovery used local repository code and golden CSVs only. This report describes observed contracts and recommended decomposition; row counts and profiles were computed from `golden/outputs/<drop>/` and `golden/inputs/<drop>/`.

[Inferred] Treat the requested dev/prod catalogs, PR schemas, Unity Catalog governance and Devin-CI promotion as deployment requirements supplied by the brief, not runtime facts verified here; model names below are logical schema.table names. `src/parity/parity_check.py:120-135`

## §1 Sources and evidence index

Citation roots: `L:` = legacy repo `daiwt-02-enterprise-etl`; unprefixed paths = this repo.
| evidence | use |
| --- | --- |
| [Verified] `golden/README.md:43-81` | Input delta, temporal-history and serialization contract |
| [Verified] `golden/manifest.json:278-293` | CSV formats and NULL marker |
| [Verified] golden profile (`golden/outputs/<drop>/`) | All consumed file metadata; individual entries cited below |
| [Verified] golden profile (`golden/outputs/<drop>/`) | Exact registered names, paths and column order |
| [Verified] `golden/capture_sqlserver.py:661-700` | Reachable capture ETL dispatch and control flow |
| [Verified] `L:wwi-ssis/wwi-ssis/DailyETLMain.dtsx:76-89` | Original native SSIS Date task; entity tasks are cited below |
| [Verified] golden profile (`golden/outputs/<drop>/`) | Per-key cross-drop comparisons; full results in profile |
| [Verified] golden profile (`golden/outputs/<drop>/`) | Additional row-level/group checks across all drops |
| [Verified] `L:README.md:12-30` | Original native model versus expanded estate scope |


## §2 Master target table
| layer | table | grain | key(s) | source(s) | golden path | SCD type | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bronze | bronze.application_cities | source row / version occurrence per drop | CityID | inputs/D0/WideWorldImporters.Application.Cities.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.Cities.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1296-1324` |
| bronze | bronze.application_cities_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Application.Cities_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.Cities_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1325-1358` |
| bronze | bronze.application_countries | source row / version occurrence per drop | CountryID | inputs/D0/WideWorldImporters.Application.Countries.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.Countries.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1359-1393` |
| bronze | bronze.application_countries_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Application.Countries_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.Countries_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1394-1439` |
| bronze | bronze.application_payment_methods | source row / version occurrence per drop | PaymentMethodID | inputs/D0/WideWorldImporters.Application.PaymentMethods.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.PaymentMethods.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1440-1465` |
| bronze | bronze.application_payment_methods_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Application.PaymentMethods_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.PaymentMethods_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1466-1493` |
| bronze | bronze.application_people | source row / version occurrence per drop | PersonID | inputs/D0/WideWorldImporters.Application.People.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.People.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1494-1535` |
| bronze | bronze.application_people_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Application.People_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.People_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1536-1595` |
| bronze | bronze.application_state_provinces | source row / version occurrence per drop | StateProvinceID | inputs/D0/WideWorldImporters.Application.StateProvinces.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.StateProvinces.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1596-1626` |
| bronze | bronze.application_state_provinces_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Application.StateProvinces_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.StateProvinces_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1627-1664` |
| bronze | bronze.application_transaction_types | source row / version occurrence per drop | TransactionTypeID | inputs/D0/WideWorldImporters.Application.TransactionTypes.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.TransactionTypes.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1665-1690` |
| bronze | bronze.application_transaction_types_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Application.TransactionTypes_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Application.TransactionTypes_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1691-1718` |
| bronze | bronze.purchasing_purchase_order_lines | source row / version occurrence per drop | PurchaseOrderLineID | inputs/D0/WideWorldImporters.Purchasing.PurchaseOrderLines.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Purchasing.PurchaseOrderLines.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1719-1751` |
| bronze | bronze.purchasing_purchase_orders | source row / version occurrence per drop | PurchaseOrderID | inputs/D0/WideWorldImporters.Purchasing.PurchaseOrders.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Purchasing.PurchaseOrders.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1752-1784` |
| bronze | bronze.purchasing_supplier_categories | source row / version occurrence per drop | SupplierCategoryID | inputs/D0/WideWorldImporters.Purchasing.SupplierCategories.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Purchasing.SupplierCategories.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1785-1810` |
| bronze | bronze.purchasing_supplier_categories_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Purchasing.SupplierCategories_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Purchasing.SupplierCategories_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1811-1838` |
| bronze | bronze.purchasing_supplier_transactions | source row / version occurrence per drop | SupplierTransactionID | inputs/D0/WideWorldImporters.Purchasing.SupplierTransactions.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Purchasing.SupplierTransactions.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1839-1874` |
| bronze | bronze.purchasing_suppliers | source row / version occurrence per drop | SupplierID | inputs/D0/WideWorldImporters.Purchasing.Suppliers.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Purchasing.Suppliers.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:1875-1924` |
| bronze | bronze.purchasing_suppliers_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Purchasing.Suppliers_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Purchasing.Suppliers_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:1925-2000` |
| bronze | bronze.sales_buying_groups | source row / version occurrence per drop | BuyingGroupID | inputs/D0/WideWorldImporters.Sales.BuyingGroups.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.BuyingGroups.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2001-2026` |
| bronze | bronze.sales_buying_groups_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Sales.BuyingGroups_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.BuyingGroups_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:2027-2054` |
| bronze | bronze.sales_customer_categories | source row / version occurrence per drop | CustomerCategoryID | inputs/D0/WideWorldImporters.Sales.CustomerCategories.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.CustomerCategories.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2055-2080` |
| bronze | bronze.sales_customer_categories_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Sales.CustomerCategories_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.CustomerCategories_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:2081-2108` |
| bronze | bronze.sales_customer_transactions | source row / version occurrence per drop | CustomerTransactionID | inputs/D0/WideWorldImporters.Sales.CustomerTransactions.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.CustomerTransactions.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2109-2143` |
| bronze | bronze.sales_customers | source row / version occurrence per drop | CustomerID | inputs/D0/WideWorldImporters.Sales.Customers.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.Customers.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2144-2195` |
| bronze | bronze.sales_customers_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Sales.Customers_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.Customers_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:2196-2275` |
| bronze | bronze.sales_invoice_lines | source row / version occurrence per drop | InvoiceLineID | inputs/D0/WideWorldImporters.Sales.InvoiceLines.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.InvoiceLines.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2276-2309` |
| bronze | bronze.sales_invoices | source row / version occurrence per drop | InvoiceID | inputs/D0/WideWorldImporters.Sales.Invoices.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.Invoices.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2310-2355` |
| bronze | bronze.sales_order_lines | source row / version occurrence per drop | OrderLineID | inputs/D0/WideWorldImporters.Sales.OrderLines.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.OrderLines.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2356-2388` |
| bronze | bronze.sales_orders | source row / version occurrence per drop | OrderID | inputs/D0/WideWorldImporters.Sales.Orders.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Sales.Orders.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2389-2425` |
| bronze | bronze.warehouse_colors | source row / version occurrence per drop | ColorID | inputs/D0/WideWorldImporters.Warehouse.Colors.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.Colors.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2426-2451` |
| bronze | bronze.warehouse_colors_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Warehouse.Colors_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.Colors_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:2452-2479` |
| bronze | bronze.warehouse_package_types | source row / version occurrence per drop | PackageTypeID | inputs/D0/WideWorldImporters.Warehouse.PackageTypes.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.PackageTypes.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2480-2505` |
| bronze | bronze.warehouse_package_types_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Warehouse.PackageTypes_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.PackageTypes_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:2506-2533` |
| bronze | bronze.warehouse_stock_item_holdings | source row / version occurrence per drop | StockItemID | inputs/D0/WideWorldImporters.Warehouse.StockItemHoldings.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.StockItemHoldings.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2534-2563` |
| bronze | bronze.warehouse_stock_item_transactions | source row / version occurrence per drop | StockItemTransactionID | inputs/D0/WideWorldImporters.Warehouse.StockItemTransactions.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.StockItemTransactions.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2564-2595` |
| bronze | bronze.warehouse_stock_items | source row / version occurrence per drop | StockItemID | inputs/D0/WideWorldImporters.Warehouse.StockItems.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.StockItems.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] primary_key; `golden/manifest.json:2596-2641` |
| bronze | bronze.warehouse_stock_items_archive | source row / version occurrence per drop | whole-row multiset (all source columns; preserve multiplicity) | inputs/D0/WideWorldImporters.Warehouse.StockItems_Archive.csv | — (input evidence: golden/inputs/D0/WideWorldImporters.Warehouse.StockItems_Archive.csv) | raw change capture | [Inferred] Append raw upserts/deletes with ingest metadata; [Verified] whole_row_multiset; `golden/manifest.json:2642-2709` |
| silver | silver.city | conformed entity-version occurrence | CityID + event timestamp + occurrence | bronze.application_countries_archive, bronze.application_countries, bronze.application_cities, bronze.application_state_provinces, bronze.application_state_provinces_archive, bronze.application_cities_archive, bronze.application_countries_archive, bronze.application_cities_archive, bronze.application_state_provinces_archive | — (downstream golden/outputs/<drop>/tables/Dimension.City.csv) | 2 | [Inferred] city and geographic attributes at each source change event; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:1-181` |
| silver | silver.customer | conformed entity-version occurrence | CustomerID + event timestamp + occurrence | bronze.sales_buying_groups_archive, bronze.sales_buying_groups, bronze.sales_customers, bronze.sales_customer_categories, bronze.application_people, bronze.sales_customer_categories_archive, bronze.sales_customers_archive, bronze.sales_buying_groups_archive, bronze.sales_customers_archive, bronze.sales_customer_categories_archive, bronze.application_people_archive | — (downstream golden/outputs/<drop>/tables/Dimension.Customer.csv) | 2 | [Inferred] customer, bill-to name, category, buying group, primary contact and postal code; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCustomerUpdates.sql:1-196` |
| silver | silver.employee | conformed entity-version occurrence | PersonID + version/event timestamp + occurrence | bronze.application_people_archive, bronze.application_people, bronze.application_people_archive | — (downstream golden/outputs/<drop>/tables/Dimension.Employee.csv) | 2 | [Inferred] employee persons only, preferred name, salesperson flag and photo; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetEmployeeUpdates.sql:1-86` |
| silver | silver.payment_method | conformed entity-version occurrence | PaymentMethodID + version timestamp + occurrence | bronze.application_payment_methods_archive, bronze.application_payment_methods, bronze.application_payment_methods_archive | — (downstream golden/outputs/<drop>/tables/Dimension.Payment_Method.csv) | 2 | [Inferred] payment method name and validity; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPaymentMethodUpdates.sql:1-78` |
| silver | silver.stock_item | conformed entity-version occurrence | StockItemID + version timestamp + occurrence | bronze.warehouse_stock_items_archive, bronze.warehouse_stock_items, bronze.warehouse_package_types, bronze.warehouse_colors, bronze.warehouse_stock_items_archive, bronze.warehouse_package_types_archive, bronze.warehouse_colors_archive | — (downstream golden/outputs/<drop>/tables/Dimension.Stock_Item.csv) | 2 | [Inferred] stock item attributes, packages, color, prices, rates and weight; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockItemUpdates.sql:1-112` |
| silver | silver.supplier | conformed entity-version occurrence | SupplierID + event/version timestamp + occurrence | bronze.purchasing_supplier_categories_archive, bronze.purchasing_supplier_categories, bronze.purchasing_suppliers, bronze.application_people, bronze.purchasing_suppliers_archive, bronze.purchasing_supplier_categories_archive, bronze.purchasing_suppliers_archive, bronze.application_people_archive | — (downstream golden/outputs/<drop>/tables/Dimension.Supplier.csv) | 2 | [Inferred] supplier category, contact, reference, payment terms and postal code; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSupplierUpdates.sql:1-135` |
| silver | silver.transaction_type | conformed entity-version occurrence | TransactionTypeID + version timestamp + occurrence | bronze.application_transaction_types_archive, bronze.application_transaction_types, bronze.application_transaction_types_archive | — (downstream golden/outputs/<drop>/tables/Dimension.Transaction_Type.csv) | 2 | [Inferred] transaction type label and validity; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionTypeUpdates.sql:1-78` |
| silver | silver.movement | conformed extract row / drop | StockItemTransactionID | bronze.warehouse_stock_item_transactions | — (downstream golden/outputs/<drop>/tables/Fact.Movement.csv) | 1/upsert | [Inferred] stock movement including signed quantity and optional invoice/purchase/customer/supplier references; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetMovementUpdates.sql:1-27` |
| silver | silver.order | conformed extract row / drop | OrderLineID retained in silver; OrderID is parent replacement key | bronze.sales_orders, bronze.sales_order_lines, bronze.warehouse_package_types, bronze.sales_customers | — (downstream golden/outputs/<drop>/tables/Fact.Order.csv) | parent replacement | [Inferred] order-line amounts, picked date, sales/picker IDs and customer geography; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetOrderUpdates.sql:1-42` |
| silver | silver.purchase | conformed extract row / drop | PurchaseOrderLineID + extract occurrence retained in silver; PurchaseOrderID is parent replacement key | bronze.purchasing_purchase_orders, bronze.purchasing_purchase_order_lines, bronze.warehouse_stock_items, bronze.warehouse_package_types | — (downstream golden/outputs/<drop>/tables/Fact.Purchase.csv) | parent replacement | [Inferred] purchase-line outers, quantity conversion, received outers and line finalization; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPurchaseUpdates.sql:1-34` |
| silver | silver.sale | conformed extract row / drop | InvoiceLineID retained in silver; InvoiceID is parent replacement key | bronze.sales_invoices, bronze.sales_invoice_lines, bronze.warehouse_stock_items, bronze.warehouse_package_types, bronze.sales_customers | — (downstream golden/outputs/<drop>/tables/Fact.Sale.csv) | parent replacement | [Inferred] invoice-line sales, tax, profit, dry/chiller quantity, delivery and customer roles; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSaleUpdates.sql:1-47` |
| silver | silver.stock_holding | conformed extract row / drop | StockItemID per drop | bronze.warehouse_stock_item_holdings | — (downstream golden/outputs/<drop>/tables/Fact.Stock_Holding.csv) | snapshot | [Inferred] full stock holding snapshot: quantity, bin, stocktake, cost and reorder targets; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockHoldingUpdates.sql:1-20` |
| silver | silver.transaction | conformed extract row / drop | source kind + CustomerTransactionID or SupplierTransactionID + extraction drop/occurrence | bronze.sales_customer_transactions, bronze.sales_invoices, bronze.purchasing_supplier_transactions | — (downstream golden/outputs/<drop>/tables/Fact.Transaction.csv) | append event | [Inferred] customer/supplier financial transaction extraction events with customer/bill-to roles and balances; replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionUpdates.sql:1-58` |
| silver | silver.date | calendar day | date | generated calendar | — (downstream golden/outputs/<drop>/tables/Dimension.Date.csv) | 0 | [Inferred] Deterministic calendar; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/PopulateDateDimensionForYear.sql:9-19` |
| gold | gold.dim_city | legacy dimension version / surrogate row | city_key; business ID: wwi_city_id (versioned) | silver.city | golden/outputs/D2/tables/Dimension.City.csv | 2 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7305-7308` |
| gold | gold.dim_customer | legacy dimension version / surrogate row | customer_key; business ID: wwi_customer_id (versioned) | silver.customer | golden/outputs/D2/tables/Dimension.Customer.csv | 2 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7325-7328` |
| gold | gold.dim_date | calendar day | date | silver.date | golden/outputs/D2/tables/Dimension.Date.csv | 0 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7342-7345` |
| gold | gold.dim_employee | legacy dimension version / surrogate row | employee_key; business ID: wwi_employee_id (versioned) | silver.employee | golden/outputs/D2/tables/Dimension.Employee.csv | 2 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7362-7365` |
| gold | gold.dim_payment_method | legacy dimension version / surrogate row | payment_method_key; business ID: wwi_payment_method_id (versioned) | silver.payment_method | golden/outputs/D2/tables/Dimension.Payment_Method.csv | 2 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7377-7380` |
| gold | gold.dim_stock_item | legacy dimension version / surrogate row | stock_item_key; business ID: wwi_stock_item_id (versioned) | silver.stock_item | golden/outputs/D2/tables/Dimension.Stock_Item.csv | 2 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7389-7392` |
| gold | gold.dim_supplier | legacy dimension version / surrogate row | supplier_key; business ID: wwi_supplier_id (versioned) | silver.supplier | golden/outputs/D2/tables/Dimension.Supplier.csv | 2 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7415-7418` |
| gold | gold.dim_transaction_type | legacy dimension version / surrogate row | transaction_type_key; business ID: wwi_transaction_type_id (versioned) | silver.transaction_type | golden/outputs/D2/tables/Dimension.Transaction_Type.csv | 2 | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7432-7435` |
| gold | gold.fact_movement | one stock-item transaction | movement_key; source identity: StockItemTransactionID | silver.movement + gold dimensions | golden/outputs/D2/tables/Fact.Movement.csv | 1/upsert | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7444-7447` |
| gold | gold.fact_order | one extracted order line / replacement occurrence | order_key; source identity: OrderLineID retained in silver; OrderID is parent replacement key | silver.order + gold dimensions | golden/outputs/D2/tables/Fact.Order.csv | parent replacement | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7461-7464` |
| gold | gold.fact_purchase | one extracted purchase line / replacement occurrence | purchase_key; source identity: PurchaseOrderLineID + extract occurrence retained in silver; PurchaseOrderID is parent replacement key | silver.purchase + gold dimensions | golden/outputs/D2/tables/Fact.Purchase.csv | parent replacement | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7486-7489` |
| gold | gold.fact_sale | one extracted invoice line / replacement occurrence | sale_key; source identity: InvoiceLineID retained in silver; InvoiceID is parent replacement key | silver.sale + gold dimensions | golden/outputs/D2/tables/Fact.Sale.csv | parent replacement | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7503-7506` |
| gold | gold.fact_stock_holding | one stock item in the current drop snapshot | stock_holding_key; source identity: StockItemID per drop | silver.stock_holding + gold dimensions | golden/outputs/D2/tables/Fact.Stock_Holding.csv | snapshot | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7530-7533` |
| gold | gold.fact_transaction | one extracted financial transaction event / drop | transaction_key; source identity: source kind + CustomerTransactionID or SupplierTransactionID + extraction drop/occurrence | silver.transaction + gold dimensions | golden/outputs/D2/tables/Fact.Transaction.csv | append event | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7545-7548` |
| gold | gold.report_sale_by_day | invoice date | invoice_date_key | gold.fact_sale | golden/outputs/D2/reports/sale-by-day.csv | derived | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7569-7572` |
| gold | gold.report_customer_history | customer dimension version | customer_key | gold.dim_customer | golden/outputs/D2/reports/customer-history.csv | derived | [Inferred] Publish exact registered columns; [Verified] `golden/manifest.json:7578-7581` |
| control | control.etl_cutoff | latest committed cutoff per entity | table_name | job run state / entity completion | golden/outputs/<drop>/tables/Integration.ETL_Cutoff.csv | control state | [Inferred] Platform-owned, not business gold; [Verified] comparison dispatch is manifest.tables only: `src/parity/parity_check.py:27-47`; golden profile (`golden/outputs/<drop>/`) |
| control | control.lineage | entity load attempt | lineage_key | job run state / entity completion | golden/outputs/<drop>/tables/Integration.Lineage.csv | control state | [Inferred] Platform-owned, not business gold; [Verified] comparison dispatch is manifest.tables only: `src/parity/parity_check.py:27-47`; golden profile (`golden/outputs/<drop>/`) |


## §3 Bronze detail

[Verified] D0 is full source input; later files are changed/inserted whole-row deltas. PK deletions carry only keys; archive/no-PK deletions carry full rows with multiplicity. Header-only upserts mean unchanged; absent deletion files mean no recorded deletions. `golden/README.md:43-62`

[Inferred] Append raw records with `_drop`, `_operation` (upsert/delete), `_source_file`, `_source_row_number`, `_ingested_at`, `_file_sha256` and an occurrence identifier; retain original formatted values. Reconstruct current source state by applying deletions and then upserts per drop, retaining multiset counts for no-PK archives. These metadata are new technical fields, not golden columns. `golden/README.md:43-78`; `golden/manifest.json:268-277`

[Verified] Below `U/D` means upsert-file rows / deletion-file rows, not cumulative state; `0*` means deletion file absent. Each path is under `golden/inputs/<drop>/`. Exact no-PK identity is **all columns in that entry’s key_columns**, not an invented business PK. `golden/README.md:43-62`
| D0 file → same filename each drop | target | PK / exact key_columns | key semantics | D0 U/D | D1 U/D | D2 U/D | evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| WideWorldImporters.Application.Cities.csv | bronze.application_cities | CityID | primary_key | 37940/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1296-1324`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.Cities_Archive.csv | bronze.application_cities_archive | all columns: CityID, CityName, StateProvinceID, Location, LatestRecordedPopulation, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 28/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1325-1358`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.Countries.csv | bronze.application_countries | CountryID | primary_key | 190/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1359-1393`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.Countries_Archive.csv | bronze.application_countries_archive | all columns: CountryID, CountryName, FormalName, IsoAlpha3Code, IsoNumericCode, CountryType, LatestRecordedPopulation, Continent, Region, Subregion, Border, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 37/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1394-1439`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.PaymentMethods.csv | bronze.application_payment_methods | PaymentMethodID | primary_key | 4/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1440-1465`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.PaymentMethods_Archive.csv | bronze.application_payment_methods_archive | all columns: PaymentMethodID, PaymentMethodName, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 1/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1466-1493`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.People.csv | bronze.application_people | PersonID | primary_key | 1111/0* | 2/0* | 7/0* | [Verified] `golden/manifest.json:1494-1535`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.People_Archive.csv | bronze.application_people_archive | all columns: PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, LogonName, IsExternalLogonProvider, HashedPassword, IsSystemUser, IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, EmailAddress, Photo, CustomFields, OtherLanguages, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 961/0* | 2/0* | 6/0* | [Verified] `golden/manifest.json:1536-1595`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.StateProvinces.csv | bronze.application_state_provinces | StateProvinceID | primary_key | 53/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1596-1626`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.StateProvinces_Archive.csv | bronze.application_state_provinces_archive | all columns: StateProvinceID, StateProvinceCode, StateProvinceName, CountryID, SalesTerritory, Border, LatestRecordedPopulation, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 104/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1627-1664`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.TransactionTypes.csv | bronze.application_transaction_types | TransactionTypeID | primary_key | 13/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1665-1690`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Application.TransactionTypes_Archive.csv | bronze.application_transaction_types_archive | all columns: TransactionTypeID, TransactionTypeName, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 1/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1691-1718`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Purchasing.PurchaseOrderLines.csv | bronze.purchasing_purchase_order_lines | PurchaseOrderLineID | primary_key | 8367/0* | 57/0* | 56/0* | [Verified] `golden/manifest.json:1719-1751`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Purchasing.PurchaseOrders.csv | bronze.purchasing_purchase_orders | PurchaseOrderID | primary_key | 2074/0* | 14/0* | 14/0* | [Verified] `golden/manifest.json:1752-1784`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Purchasing.SupplierCategories.csv | bronze.purchasing_supplier_categories | SupplierCategoryID | primary_key | 9/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1785-1810`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Purchasing.SupplierCategories_Archive.csv | bronze.purchasing_supplier_categories_archive | all columns: SupplierCategoryID, SupplierCategoryName, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 1/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1811-1838`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Purchasing.SupplierTransactions.csv | bronze.purchasing_supplier_transactions | SupplierTransactionID | primary_key | 2438/0* | 16/0* | 14/0* | [Verified] `golden/manifest.json:1839-1874`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Purchasing.Suppliers.csv | bronze.purchasing_suppliers | SupplierID | primary_key | 13/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1875-1924`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Purchasing.Suppliers_Archive.csv | bronze.purchasing_suppliers_archive | all columns: SupplierID, SupplierName, SupplierCategoryID, PrimaryContactPersonID, AlternateContactPersonID, DeliveryMethodID, DeliveryCityID, PostalCityID, SupplierReference, BankAccountName, BankAccountBranch, BankAccountCode, BankAccountNumber, BankInternationalCode, PaymentDays, InternalComments, PhoneNumber, FaxNumber, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 13/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:1925-2000`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.BuyingGroups.csv | bronze.sales_buying_groups | BuyingGroupID | primary_key | 2/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2001-2026`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.BuyingGroups_Archive.csv | bronze.sales_buying_groups_archive | all columns: BuyingGroupID, BuyingGroupName, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 0/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2027-2054`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.CustomerCategories.csv | bronze.sales_customer_categories | CustomerCategoryID | primary_key | 8/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2055-2080`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.CustomerCategories_Archive.csv | bronze.sales_customer_categories_archive | all columns: CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 1/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2081-2108`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.CustomerTransactions.csv | bronze.sales_customer_transactions | CustomerTransactionID | primary_key | 97147/0* | 689/0* | 828/0* | [Verified] `golden/manifest.json:2109-2143`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.Customers.csv | bronze.sales_customers | CustomerID | primary_key | 663/0* | 0/0* | 1/0* | [Verified] `golden/manifest.json:2144-2195`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.Customers_Archive.csv | bronze.sales_customers_archive | all columns: CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, DeliveryRun, RunPosition, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 51/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2196-2275`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.InvoiceLines.csv | bronze.sales_invoice_lines | InvoiceLineID | primary_key | 228265/0* | 1414/0* | 1807/0* | [Verified] `golden/manifest.json:2276-2309`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.Invoices.csv | bronze.sales_invoices | InvoiceID | primary_key | 70510/0* | 519/0* | 639/0* | [Verified] `golden/manifest.json:2310-2355`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.OrderLines.csv | bronze.sales_order_lines | OrderLineID | primary_key | 231412/0* | 1452/0* | 1852/0* | [Verified] `golden/manifest.json:2356-2388`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Sales.Orders.csv | bronze.sales_orders | OrderID | primary_key | 73595/0* | 473/0* | 601/0* | [Verified] `golden/manifest.json:2389-2425`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.Colors.csv | bronze.warehouse_colors | ColorID | primary_key | 36/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2426-2451`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.Colors_Archive.csv | bronze.warehouse_colors_archive | all columns: ColorID, ColorName, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 1/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2452-2479`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.PackageTypes.csv | bronze.warehouse_package_types | PackageTypeID | primary_key | 14/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2480-2505`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.PackageTypes_Archive.csv | bronze.warehouse_package_types_archive | all columns: PackageTypeID, PackageTypeName, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 0/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2506-2533`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.StockItemHoldings.csv | bronze.warehouse_stock_item_holdings | StockItemID | primary_key | 227/0* | 227/0* | 227/0* | [Verified] `golden/manifest.json:2534-2563`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.StockItemTransactions.csv | bronze.warehouse_stock_item_transactions | StockItemTransactionID | primary_key | 236667/0* | 1462/0* | 1854/0* | [Verified] `golden/manifest.json:2564-2595`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.StockItems.csv | bronze.warehouse_stock_items | StockItemID | primary_key | 227/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2596-2641`; golden profile (`golden/outputs/<drop>/`) |
| WideWorldImporters.Warehouse.StockItems_Archive.csv | bronze.warehouse_stock_items_archive | all columns: StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, TypicalWeightPerUnit, MarketingComments, InternalComments, Photo, CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo | whole_row_multiset | 444/0* | 0/0* | 0/0* | [Verified] `golden/manifest.json:2642-2709`; golden profile (`golden/outputs/<drop>/`) |


## §4 Silver detail and non-destructive data quality

[Inferred] Each silver table retains source business IDs and extract drop/occurrence; dimensional rows retain exact validity strings plus an internal occurrence ID, while fact line IDs remain available even where the gold contract omits them. This supports complete source-to-gold traceability without changing golden columns. `golden/README.md:56-81`; `golden/capture_sqlserver.py:675-687`

[Inferred] **WARN** records expectation metrics while passing the row; **QUARANTINE-COPY** writes a diagnostic copy and passes the original; **DROP** is not proposed for business data. Key presence, referential resolution, interval order and range checks must not remove any row visible in the golden baseline. Parsing errors retain raw payload for diagnosis and require an explicit processing decision before promotion. `src/parity/parity_check.py:49-57`; `golden/README.md:56-62`

### silver.city

[Inferred] Holds city and geographic attributes at each source change event. Grain/key: CityID + event timestamp + occurrence. Inputs: Application.Countries_Archive, Application.Countries, Application.Cities, Application.StateProvinces, Application.StateProvinces_Archive, Application.Cities_Archive, Application.Countries_Archive, Application.Cities_Archive, Application.StateProvinces_Archive. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:1-181`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL CityID/event time; temporal joins to state/country; population >=0; geography parse; validity order. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:1-181`; `src/parity/parity_check.py:49-57`

[Verified] Extract produces versioned attribute rows and returns them ordered by Valid From; current/archived source history participates in the extract. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:1-181`

### silver.customer

[Inferred] Holds customer, bill-to name, category, buying group, primary contact and postal code. Grain/key: CustomerID + event timestamp + occurrence. Inputs: Sales.BuyingGroups_Archive, Sales.BuyingGroups, Sales.Customers, Sales.CustomerCategories, Application.People, Sales.CustomerCategories_Archive, Sales.Customers_Archive, Sales.BuyingGroups_Archive, Sales.Customers_Archive, Sales.CustomerCategories_Archive, Application.People_Archive. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCustomerUpdates.sql:1-196`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL CustomerID/event time; customer category, bill-to customer, buying-group and contact references; postal codes remain strings; validity order. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCustomerUpdates.sql:1-196`; `src/parity/parity_check.py:49-57`

[Verified] Extract produces versioned attribute rows and returns them ordered by Valid From; current/archived source history participates in the extract. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCustomerUpdates.sql:1-196`

### silver.employee

[Inferred] Holds employee persons only, preferred name, salesperson flag and photo. Grain/key: PersonID + version/event timestamp + occurrence. Inputs: Application.People_Archive, Application.People, Application.People_Archive. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetEmployeeUpdates.sql:1-86`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL PersonID/event time; IsEmployee inclusion follows extract; IsSalesperson in {0,1}; validity order; nullable Photo accepted. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetEmployeeUpdates.sql:1-86`; `src/parity/parity_check.py:49-57`

[Verified] Extract produces versioned attribute rows and returns them ordered by Valid From; current/archived source history participates in the extract. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetEmployeeUpdates.sql:1-86`

### silver.payment_method

[Inferred] Holds payment method name and validity. Grain/key: PaymentMethodID + version timestamp + occurrence. Inputs: Application.PaymentMethods_Archive, Application.PaymentMethods, Application.PaymentMethods_Archive. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPaymentMethodUpdates.sql:1-78`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL PaymentMethodID/event time; validity order; name presence. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPaymentMethodUpdates.sql:1-78`; `src/parity/parity_check.py:49-57`

[Verified] Extract produces versioned attribute rows and returns them ordered by Valid From; current/archived source history participates in the extract. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPaymentMethodUpdates.sql:1-78`

### silver.stock_item

[Inferred] Holds stock item attributes, packages, color, prices, rates and weight. Grain/key: StockItemID + version timestamp + occurrence. Inputs: Warehouse.StockItems_Archive, Warehouse.StockItems, Warehouse.PackageTypes, Warehouse.Colors, Warehouse.StockItems_Archive, Warehouse.PackageTypes_Archive, Warehouse.Colors_Archive. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockItemUpdates.sql:1-112`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL StockItemID/event time; package/color references; rates/prices/weights nonnegative and chiller flag in {0,1}; zero/default quantities allowed; validity order. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockItemUpdates.sql:1-112`; `src/parity/parity_check.py:49-57`

[Verified] Extract produces versioned attribute rows and returns them ordered by Valid From; current/archived source history participates in the extract. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockItemUpdates.sql:1-112`

### silver.supplier

[Inferred] Holds supplier category, contact, reference, payment terms and postal code. Grain/key: SupplierID + event/version timestamp + occurrence. Inputs: Purchasing.SupplierCategories_Archive, Purchasing.SupplierCategories, Purchasing.Suppliers, Application.People, Purchasing.Suppliers_Archive, Purchasing.SupplierCategories_Archive, Purchasing.Suppliers_Archive, Application.People_Archive. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSupplierUpdates.sql:1-135`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL SupplierID/event time; category/contact references; payment days >=0; postal/reference values remain strings; validity order. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSupplierUpdates.sql:1-135`; `src/parity/parity_check.py:49-57`

[Verified] Extract produces versioned attribute rows and returns them ordered by Valid From; current/archived source history participates in the extract. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSupplierUpdates.sql:1-135`

### silver.transaction_type

[Inferred] Holds transaction type label and validity. Grain/key: TransactionTypeID + version timestamp + occurrence. Inputs: Application.TransactionTypes_Archive, Application.TransactionTypes, Application.TransactionTypes_Archive. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionTypeUpdates.sql:1-78`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL TransactionTypeID/event time; name presence; validity order. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionTypeUpdates.sql:1-78`; `src/parity/parity_check.py:49-57`

[Verified] Extract produces versioned attribute rows and returns them ordered by Valid From; current/archived source history participates in the extract. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionTypeUpdates.sql:1-78`

### silver.movement

[Inferred] Holds stock movement including signed quantity and optional invoice/purchase/customer/supplier references. Grain/key: StockItemTransactionID. Inputs: Warehouse.StockItemTransactions. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetMovementUpdates.sql:1-27`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL StockItemTransactionID; stock/type reference resolution; customer/supplier/invoice/PO optional; signed quantities accepted, not constrained nonnegative. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetMovementUpdates.sql:1-27`; `src/parity/parity_check.py:49-57`

[Verified] Extract content and filtering are defined by the SQL above; native result-set declaration supplies captured names and SQL shapes. `golden/native_result_sets.json:5`

### silver.order

[Inferred] Holds order-line amounts, picked date, sales/picker IDs and customer geography. Grain/key: OrderLineID retained in silver; OrderID is parent replacement key. Inputs: Sales.Orders, Sales.OrderLines, Warehouse.PackageTypes, Sales.Customers. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetOrderUpdates.sql:1-42`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL OrderLineID/OrderID; parent/customer/package/item references; arithmetic consistency of rounded totals; picked date optional; quantity/rate/price range metrics. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetOrderUpdates.sql:1-42`; `src/parity/parity_check.py:49-57`

[Verified] Extract content and filtering are defined by the SQL above; native result-set declaration supplies captured names and SQL shapes. `golden/native_result_sets.json:6`

### silver.purchase

[Inferred] Holds purchase-line outers, quantity conversion, received outers and line finalization. Grain/key: PurchaseOrderLineID + extract occurrence retained in silver; PurchaseOrderID is parent replacement key. Inputs: Purchasing.PurchaseOrders, Purchasing.PurchaseOrderLines, Warehouse.StockItems, Warehouse.PackageTypes. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPurchaseUpdates.sql:1-34`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL PurchaseOrderLineID/PurchaseOrderID; parent/supplier/item/package references; ordered_quantity = ordered_outers * source quantity_per_outer; received/finalization ranges as metrics. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPurchaseUpdates.sql:1-34`; `src/parity/parity_check.py:49-57`

[Verified] Extract content and filtering are defined by the SQL above; native result-set declaration supplies captured names and SQL shapes. `golden/native_result_sets.json:8`

### silver.sale

[Inferred] Holds invoice-line sales, tax, profit, dry/chiller quantity, delivery and customer roles. Grain/key: InvoiceLineID retained in silver; InvoiceID is parent replacement key. Inputs: Sales.Invoices, Sales.InvoiceLines, Warehouse.StockItems, Warehouse.PackageTypes, Sales.Customers. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSaleUpdates.sql:1-47`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL InvoiceLineID/InvoiceID; parent/customer/bill-to/item/package references; totals = excluding tax + tax; dry/chiller split; delivery date optional. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSaleUpdates.sql:1-47`; `src/parity/parity_check.py:49-57`

[Verified] Extract content and filtering are defined by the SQL above; native result-set declaration supplies captured names and SQL shapes. `golden/native_result_sets.json:9`

### silver.stock_holding

[Inferred] Holds full stock holding snapshot: quantity, bin, stocktake, cost and reorder targets. Grain/key: StockItemID per drop. Inputs: Warehouse.StockItemHoldings. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockHoldingUpdates.sql:1-20`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: NOT NULL StockItemID; stock-item reference; quantity, stocktake, reorder and target bounds as metrics (not rejection); current-stock reference check. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockHoldingUpdates.sql:1-20`; `src/parity/parity_check.py:49-57`

[Verified] Extract content and filtering are defined by the SQL above; native result-set declaration supplies captured names and SQL shapes. `golden/native_result_sets.json:10`

### silver.transaction

[Inferred] Holds customer/supplier financial transaction extraction events with customer/bill-to roles and balances. Grain/key: source kind + CustomerTransactionID or SupplierTransactionID + extraction drop/occurrence. Inputs: Sales.CustomerTransactions, Sales.Invoices, Purchasing.SupplierTransactions. Replaces `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionUpdates.sql:1-58`

[Inferred] Expectations, all **WARN or QUARANTINE-COPY, never DROP**: Exactly one non-NULL customer/supplier transaction ID; transaction kind-qualified source ID; role-specific optional references; total arithmetic; finalization bit; signed balances accepted. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionUpdates.sql:1-58`; `src/parity/parity_check.py:49-57`

[Verified] Extract content and filtering are defined by the SQL above; native result-set declaration supplies captured names and SQL shapes. `golden/native_result_sets.json:13`

### silver.date

[Inferred] One calendar row per date; key `date`; generate the captured 2013–2016 range with matching calendar/fiscal labels. WARN/QUARANTINE-COPY on absent dates, duplicate date or out-of-range attributes; no business row dropping. golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/PopulateDateDimensionForYear.sql:9-19`

## §5 Gold contracts, keys and dimensions/facts/reports

[Verified] Column tables below preserve the golden CSV order exactly. Observed types/null counts refer to D2, not future maximum widths or DDL constraints; integer fit is observational even when the legacy fact key is BIGINT. Decimal precision/scale are observed and native legacy declarations can be wider. Literal `\N` is parsed as NULL; dates are `YYYY-MM-DD`, datetime values use seven fractional digits, spatial values are WKT and binary values use hex per capture conventions. `golden/manifest.json:278-293`; `src/parity/parity_check.py:105-117`

[Verified] Parity hashes every selected registered column, including surrogate keys and lineage_key; it compares row counts and order-independent serialized-row digests. Therefore published values must match exactly under the current comparison. `src/parity/parity_check.py:22-24`; `src/parity/parity_check.py:49-57`; `src/parity/parity_check.py:127-135`

[Inferred] Preserve legacy key mappings for this captured baseline and allocate subsequent keys in a persisted, explicitly ordered map per entity/drop; do not replace published keys with unrelated hashes. Keep technical ingest/quality fields outside the registered gold projection. `src/parity/parity_check.py:127-133`; golden profile (`golden/outputs/<drop>/`)

[Verified] Seven dimensions use sequence defaults starting at 1; facts use BIGINT IDENTITY(1,1). SQL result ORDER BY controls extract order, but migration INSERT SELECT has no explicit ORDER BY; observed captured assignments, rather than a new universal ordering assumption, define the reproduction map. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:1-3`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/CustomerKey.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:1-3`; `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:172-176`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:33-40`

[Verified] Every newly appearing surrogate key in D1/D2 is above that table’s prior-drop maximum; the only new dimension key is Employee 213 in D2. Stock_Holding retains the same 1–227 numeric key set instead of adding keys. golden profile (`golden/outputs/<drop>/`); golden profile (`golden/outputs/<drop>/`)

[Verified] SCD2 common mechanism: Get*Updates closes each staged version at the next **strictly later** staged valid_from, otherwise end-of-time; MigrateStaged* closes existing end-of-time rows at **MIN(staged valid_from)** per WWI ID and inserts staged rows. Example: `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCustomerUpdates.sql:178-191`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:19-40`

[Verified] Dimension end-of-time sentinel is `9999-12-31 23:59:59.9999999`; unknown rows begin `2013-01-01 00:00:00.0000000`, use key/business ID/lineage 0 and are shown in full below. `L:wwi-dw-ssdt/wwi-dw-ssdt/Application/Stored Procedures/Configuration_ReseedETL.sql:8-77`; golden profile (`golden/outputs/<drop>/`)

### gold.dim_city

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.City.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| city_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:2` |
| wwi_city_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:3` |
| city | string max length=35 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:4` |
| state_province | string max length=29 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:5` |
| country | string max length=13 | NVARCHAR (60) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:6` |
| continent | string max length=13 | NVARCHAR (30) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:7` |
| sales_territory | string max length=14 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:8` |
| region | string max length=8 | NVARCHAR (30) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:9` |
| subregion | string max length=16 | NVARCHAR (30) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:10` |
| location | string max length=41 | [sys] | observed NULLs=1 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:11` |
| latest_recorded_population | int | BIGINT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:12` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:13` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:14` |
| lineage_key | int; bit-compatible (not proof of SQL bit) | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:15` |


[Verified] Surrogate: city_key INT sequence default; business key: wwi_city_id; SCD type 2, with occurrence-specific versions. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/City.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCityData.sql:1-59`

[Verified] Sequence declaration: `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/CityKey.sql:1-4`; captured key/transition observations: golden profile (`golden/outputs/<drop>/`)

[Verified] Extract valid_from uses @ValidFrom change-event timestamp; valid_to is then recalculated from the next strictly later staged start or the exact end-of-time sentinel. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:1-181`

[Verified] Full unknown member (JSON strings preserve CSV spelling): `{"city_key": "0", "wwi_city_id": "0", "city": "Unknown", "state_province": "N/A", "country": "N/A", "continent": "N/A", "sales_territory": "N/A", "region": "N/A", "subregion": "N/A", "location": "\\N", "latest_recorded_population": "0", "valid_from": "2013-01-01 00:00:00.0000000", "valid_to": "9999-12-31 23:59:59.9999999", "lineage_key": "0"}`. `golden/outputs/D2/tables/Dimension.City.csv:2`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Publish the captured surrogate, attributes, validity and lineage values as the compatibility projection over silver; retain the version-occurrence mapping separately. `src/parity/parity_check.py:49-57`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Dimension.City.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCityData.sql:1-59`

### gold.dim_customer

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.Customer.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| customer_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:2` |
| wwi_customer_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:3` |
| customer | string max length=38 | NVARCHAR (100) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:4` |
| bill_to_customer | string max length=27 | NVARCHAR (100) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:5` |
| category | string max length=12 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:6` |
| buying_group | string max length=13 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:7` |
| primary_contact | string max length=29 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:8` |
| postal_code | string max length=5 | NVARCHAR (10) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:9` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:10` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:11` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:12` |


[Verified] Surrogate: customer_key INT sequence default; business key: wwi_customer_id; SCD type 2, with occurrence-specific versions. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Customer.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:1-57`

[Verified] Sequence declaration: `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/CustomerKey.sql:1-4`; captured key/transition observations: golden profile (`golden/outputs/<drop>/`)

[Verified] Extract valid_from uses c.ValidFrom (customer row validity); valid_to is then recalculated from the next strictly later staged start or the exact end-of-time sentinel. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCustomerUpdates.sql:1-196`

[Verified] Full unknown member (JSON strings preserve CSV spelling): `{"customer_key": "0", "wwi_customer_id": "0", "customer": "Unknown", "bill_to_customer": "N/A", "category": "N/A", "buying_group": "N/A", "primary_contact": "N/A", "postal_code": "N/A", "valid_from": "2013-01-01 00:00:00.0000000", "valid_to": "9999-12-31 23:59:59.9999999", "lineage_key": "0"}`. `golden/outputs/D2/tables/Dimension.Customer.csv:2`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Publish the captured surrogate, attributes, validity and lineage values as the compatibility projection over silver; retain the version-occurrence mapping separately. `src/parity/parity_check.py:49-57`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Dimension.Customer.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:1-57`

### gold.dim_date

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.Date.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| date | date YYYY-MM-DD | DATE | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:2` |
| day_number | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:4` |
| day | int | NVARCHAR (10) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:5` |
| month | string max length=9 | NVARCHAR (10) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:11` |
| short_month | string max length=3 | NVARCHAR (3) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:12` |
| calendar_month_number | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:31` |
| calendar_month_label | string max length=10 | NVARCHAR (20) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:32` |
| calendar_year | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:40` |
| calendar_year_label | string max length=6 | NVARCHAR (10) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:41` |
| fiscal_month_number | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:42` |
| fiscal_month_label | string max length=10 | NVARCHAR (20) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:43` |
| fiscal_year | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:48` |
| fiscal_year_label | string max length=6 | NVARCHAR (10) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:49` |
| iso_week_number | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Date.sql:63` |


[Verified] Key/business key is the `date` column (no surrogate or key-0 member in CSV); observed calendar spans 2013-01-01–2016-12-31, 1,461 rows; SCD type 0. golden profile (`golden/outputs/<drop>/`); golden profile (`golden/outputs/<drop>/`)

[Inferred] Expose only the captured 14-column calendar projection from silver.date. `golden/outputs/D2/tables/Dimension.Date.csv:1`

### gold.dim_employee

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.Employee.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| employee_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:2` |
| wwi_employee_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:3` |
| employee | string max length=18 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:4` |
| preferred_name | string max length=8 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:5` |
| is_salesperson | int; bit-compatible (not proof of SQL bit) | BIT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:6` |
| photo | unobserved (all NULL) | VARBINARY (MAX) | observed NULLs=214 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:7` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:8` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:9` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:10` |


[Verified] Surrogate: employee_key INT sequence default; business key: wwi_employee_id; SCD type 2, with occurrence-specific versions. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Employee.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedEmployeeData.sql:1-54`

[Verified] Sequence declaration: `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/EmployeeKey.sql:1-4`; captured key/transition observations: golden profile (`golden/outputs/<drop>/`)

[Verified] Extract valid_from uses p.ValidFrom (person row validity); valid_to is then recalculated from the next strictly later staged start or the exact end-of-time sentinel. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetEmployeeUpdates.sql:1-86`

[Verified] Full unknown member (JSON strings preserve CSV spelling): `{"employee_key": "0", "wwi_employee_id": "0", "employee": "Unknown", "preferred_name": "N/A", "is_salesperson": "0", "photo": "\\N", "valid_from": "2013-01-01 00:00:00.0000000", "valid_to": "9999-12-31 23:59:59.9999999", "lineage_key": "0"}`. `golden/outputs/D2/tables/Dimension.Employee.csv:2`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Publish the captured surrogate, attributes, validity and lineage values as the compatibility projection over silver; retain the version-occurrence mapping separately. `src/parity/parity_check.py:49-57`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Dimension.Employee.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedEmployeeData.sql:1-54`

### gold.dim_payment_method

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.Payment_Method.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| payment_method_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Payment Method.sql:2` |
| wwi_payment_method_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Payment Method.sql:3` |
| payment_method | string max length=11 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Payment Method.sql:4` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Payment Method.sql:5` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Payment Method.sql:6` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Payment Method.sql:7` |


[Verified] Surrogate: payment_method_key INT sequence default; business key: wwi_payment_method_id; SCD type 2, with occurrence-specific versions. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Payment Method.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedPaymentMethodData.sql:1-54`

[Verified] Sequence declaration: `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/PaymentMethodKey.sql:1-4`; captured key/transition observations: golden profile (`golden/outputs/<drop>/`)

[Verified] Extract valid_from uses p.ValidFrom (payment-method row validity); valid_to is then recalculated from the next strictly later staged start or the exact end-of-time sentinel. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPaymentMethodUpdates.sql:1-78`

[Verified] Full unknown member (JSON strings preserve CSV spelling): `{"payment_method_key": "0", "wwi_payment_method_id": "0", "payment_method": "Unknown", "valid_from": "2013-01-01 00:00:00.0000000", "valid_to": "9999-12-31 23:59:59.9999999", "lineage_key": "0"}`. `golden/outputs/D2/tables/Dimension.Payment_Method.csv:2`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Publish the captured surrogate, attributes, validity and lineage values as the compatibility projection over silver; retain the version-occurrence mapping separately. `src/parity/parity_check.py:49-57`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Dimension.Payment_Method.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedPaymentMethodData.sql:1-54`

### gold.dim_stock_item

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.Stock_Item.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| stock_item_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:2` |
| wwi_stock_item_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:3` |
| stock_item | string max length=85 | NVARCHAR (100) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:4` |
| color | string max length=11 | NVARCHAR (20) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:5` |
| selling_package | string max length=6 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:6` |
| buying_package | string max length=6 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:7` |
| brand | string max length=9 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:8` |
| size | string max length=13 | NVARCHAR (20) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:9` |
| lead_time_days | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:10` |
| quantity_per_outer | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:11` |
| is_chiller_stock | int; bit-compatible (not proof of SQL bit) | BIT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:12` |
| barcode | string max length=13 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:13` |
| tax_rate | decimal: max observed precision=5, scale=3, safe common decimal(5,3) | DECIMAL (18, 3) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:14` |
| unit_price | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:15` |
| recommended_retail_price | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:16` |
| typical_weight_per_unit | decimal: max observed precision=5, scale=3, safe common decimal(5,3) | DECIMAL (18, 3) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:17` |
| photo | unobserved (all NULL) | VARBINARY (MAX) | observed NULLs=672 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:18` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:19` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:20` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:21` |


[Verified] Surrogate: stock_item_key INT sequence default; business key: wwi_stock_item_id; SCD type 2, with occurrence-specific versions. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Stock Item.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedStockItemData.sql:1-60`

[Verified] Sequence declaration: `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/StockItemKey.sql:1-4`; captured key/transition observations: golden profile (`golden/outputs/<drop>/`)

[Verified] Extract valid_from uses si.ValidFrom (stock-item row validity); valid_to is then recalculated from the next strictly later staged start or the exact end-of-time sentinel. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockItemUpdates.sql:1-112`

[Verified] Full unknown member (JSON strings preserve CSV spelling): `{"stock_item_key": "0", "wwi_stock_item_id": "0", "stock_item": "Unknown", "color": "N/A", "selling_package": "N/A", "buying_package": "N/A", "brand": "N/A", "size": "N/A", "lead_time_days": "0", "quantity_per_outer": "0", "is_chiller_stock": "0", "barcode": "N/A", "tax_rate": "0.000", "unit_price": "0.00", "recommended_retail_price": "0.00", "typical_weight_per_unit": "0.000", "photo": "\\N", "valid_from": "2013-01-01 00:00:00.0000000", "valid_to": "9999-12-31 23:59:59.9999999", "lineage_key": "0"}`. `golden/outputs/D2/tables/Dimension.Stock_Item.csv:2`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Publish the captured surrogate, attributes, validity and lineage values as the compatibility projection over silver; retain the version-occurrence mapping separately. `src/parity/parity_check.py:49-57`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Dimension.Stock_Item.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedStockItemData.sql:1-60`

### gold.dim_supplier

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.Supplier.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| supplier_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:2` |
| wwi_supplier_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:3` |
| supplier | string max length=24 | NVARCHAR (100) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:4` |
| category | string max length=27 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:5` |
| primary_contact | string max length=18 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:6` |
| supplier_reference | string max length=11 | NVARCHAR (20) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:7` |
| payment_days | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:8` |
| postal_code | string max length=5 | NVARCHAR (10) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:9` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:10` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:11` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:12` |


[Verified] Surrogate: supplier_key INT sequence default; business key: wwi_supplier_id; SCD type 2, with occurrence-specific versions. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Supplier.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedSupplierData.sql:1-56`

[Verified] Sequence declaration: `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/SupplierKey.sql:1-4`; captured key/transition observations: golden profile (`golden/outputs/<drop>/`)

[Verified] Extract valid_from uses s.ValidFrom (supplier row validity); valid_to is then recalculated from the next strictly later staged start or the exact end-of-time sentinel. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSupplierUpdates.sql:1-135`

[Verified] Full unknown member (JSON strings preserve CSV spelling): `{"supplier_key": "0", "wwi_supplier_id": "0", "supplier": "Unknown", "category": "N/A", "primary_contact": "N/A", "supplier_reference": "N/A", "payment_days": "0", "postal_code": "N/A", "valid_from": "2013-01-01 00:00:00.0000000", "valid_to": "9999-12-31 23:59:59.9999999", "lineage_key": "0"}`. `golden/outputs/D2/tables/Dimension.Supplier.csv:2`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Publish the captured surrogate, attributes, validity and lineage values as the compatibility projection over silver; retain the version-occurrence mapping separately. `src/parity/parity_check.py:49-57`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Dimension.Supplier.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedSupplierData.sql:1-56`

### gold.dim_transaction_type

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Dimension.Transaction_Type.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| transaction_type_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Transaction Type.sql:2` |
| wwi_transaction_type_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Transaction Type.sql:3` |
| transaction_type | string max length=29 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Transaction Type.sql:4` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Transaction Type.sql:5` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | DATETIME2 (7) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Transaction Type.sql:6` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Transaction Type.sql:7` |


[Verified] Surrogate: transaction_type_key INT sequence default; business key: wwi_transaction_type_id; SCD type 2, with occurrence-specific versions. `L:wwi-dw-ssdt/wwi-dw-ssdt/Dimension/Tables/Transaction Type.sql:1-4`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedTransactionTypeData.sql:1-54`

[Verified] Sequence declaration: `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Sequences/TransactionTypeKey.sql:1-4`; captured key/transition observations: golden profile (`golden/outputs/<drop>/`)

[Verified] Extract valid_from uses p.ValidFrom (transaction-type row validity); valid_to is then recalculated from the next strictly later staged start or the exact end-of-time sentinel. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionTypeUpdates.sql:1-78`

[Verified] Full unknown member (JSON strings preserve CSV spelling): `{"transaction_type_key": "0", "wwi_transaction_type_id": "0", "transaction_type": "Unknown", "valid_from": "2013-01-01 00:00:00.0000000", "valid_to": "9999-12-31 23:59:59.9999999", "lineage_key": "0"}`. `golden/outputs/D2/tables/Dimension.Transaction_Type.csv:2`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Publish the captured surrogate, attributes, validity and lineage values as the compatibility projection over silver; retain the version-occurrence mapping separately. `src/parity/parity_check.py:49-57`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Dimension.Transaction_Type.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedTransactionTypeData.sql:1-54`

### gold.fact_movement

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Fact.Movement.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| movement_key | int | BIGINT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:2` |
| date_key | date YYYY-MM-DD | DATE | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:3` |
| stock_item_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:4` |
| customer_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:5` |
| supplier_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:6` |
| transaction_type_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:7` |
| wwi_stock_item_transaction_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:8` |
| wwi_invoice_id | int | INT | observed NULLs=8497 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:9` |
| wwi_purchase_order_id | int | INT | observed NULLs=231530 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:10` |
| quantity | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:11` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:12` |


[Verified] Surrogate: movement_key BIGINT IDENTITY(1,1). `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Movement.sql:1-3`

[Inferred] Business grain/key: StockItemTransactionID; source line identity remains in silver, with gold key mapping at published-row occurrence. SCD/update style: 1/upsert. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetMovementUpdates.sql:1-27`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedMovementData.sql:1-82`

[Verified] Legacy refresh: MERGE on WWI Stock Item Transaction ID; matched rows update including lineage, new rows insert. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedMovementData.sql:1-82`

[Verified] No own surrogate-key-0 row occurs in these fact snapshots; dimension references may use key 0 per lookup COALESCE. golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedMovementData.sql:1-82`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Fact.Movement.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedMovementData.sql:1-82`

### gold.fact_order

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Fact.Order.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| order_key | int | BIGINT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:2` |
| city_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:3` |
| customer_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:4` |
| stock_item_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:5` |
| order_date_key | date YYYY-MM-DD | DATE | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:6` |
| picked_date_key | date YYYY-MM-DD | DATE | observed NULLs=3181 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:7` |
| salesperson_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:8` |
| picker_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:9` |
| wwi_order_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:10` |
| wwi_backorder_id | int | INT | observed NULLs=213824 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:11` |
| description | string max length=85 | NVARCHAR (100) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:12` |
| package | string max length=6 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:13` |
| quantity | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:14` |
| unit_price | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:15` |
| tax_rate | decimal: max observed precision=5, scale=3, safe common decimal(5,3) | DECIMAL (18, 3) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:16` |
| total_excluding_tax | decimal: max observed precision=7, scale=2, safe common decimal(7,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:17` |
| tax_amount | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:18` |
| total_including_tax | decimal: max observed precision=7, scale=2, safe common decimal(7,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:19` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:20` |


[Verified] Surrogate: order_key BIGINT IDENTITY(1,1). `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Order.sql:1-3`

[Inferred] Business grain/key: OrderLineID retained in silver; OrderID is parent replacement key; source line identity remains in silver, with gold key mapping at published-row occurrence. SCD/update style: parent replacement. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetOrderUpdates.sql:1-42`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedOrderData.sql:1-86`

[Verified] Legacy refresh: delete matching WWI Order IDs, then insert the staged order lines. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedOrderData.sql:1-86`

[Verified] No own surrogate-key-0 row occurs in these fact snapshots; dimension references may use key 0 per lookup COALESCE. golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedOrderData.sql:1-86`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Fact.Order.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedOrderData.sql:1-86`

### gold.fact_purchase

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Fact.Purchase.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| purchase_key | int | BIGINT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:2` |
| date_key | date YYYY-MM-DD | DATE | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:3` |
| supplier_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:4` |
| stock_item_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:5` |
| wwi_purchase_order_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:6` |
| ordered_outers | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:7` |
| ordered_quantity | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:8` |
| received_outers | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:9` |
| package | string max length=6 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:10` |
| is_order_finalized | int; bit-compatible (not proof of SQL bit) | BIT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:11` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:12` |


[Verified] Surrogate: purchase_key BIGINT IDENTITY(1,1). `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Purchase.sql:1-3`

[Inferred] Business grain/key: PurchaseOrderLineID + extract occurrence retained in silver; PurchaseOrderID is parent replacement key; source line identity remains in silver, with gold key mapping at published-row occurrence. SCD/update style: parent replacement. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetPurchaseUpdates.sql:1-34`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedPurchaseData.sql:1-64`

[Verified] Legacy refresh: delete matching WWI Purchase Order IDs, then insert staged purchase lines. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedPurchaseData.sql:1-64`

[Verified] No own surrogate-key-0 row occurs in these fact snapshots; dimension references may use key 0 per lookup COALESCE. golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedPurchaseData.sql:1-64`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Fact.Purchase.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedPurchaseData.sql:1-64`

### gold.fact_sale

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Fact.Sale.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| sale_key | int | BIGINT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:2` |
| city_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:3` |
| customer_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:4` |
| bill_to_customer_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:5` |
| stock_item_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:6` |
| invoice_date_key | date YYYY-MM-DD | DATE | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:7` |
| delivery_date_key | date YYYY-MM-DD | DATE | observed NULLs=376 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:8` |
| salesperson_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:9` |
| wwi_invoice_id | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:10` |
| description | string max length=85 | NVARCHAR (100) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:11` |
| package | string max length=6 | NVARCHAR (50) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:12` |
| quantity | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:13` |
| unit_price | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:14` |
| tax_rate | decimal: max observed precision=5, scale=3, safe common decimal(5,3) | DECIMAL (18, 3) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:15` |
| total_excluding_tax | decimal: max observed precision=7, scale=2, safe common decimal(7,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:16` |
| tax_amount | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:17` |
| profit | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:18` |
| total_including_tax | decimal: max observed precision=7, scale=2, safe common decimal(7,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:19` |
| total_dry_items | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:20` |
| total_chiller_items | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:21` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:22` |


[Verified] Surrogate: sale_key BIGINT IDENTITY(1,1). `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:1-3`

[Inferred] Business grain/key: InvoiceLineID retained in silver; InvoiceID is parent replacement key; source line identity remains in silver, with gold key mapping at published-row occurrence. SCD/update style: parent replacement. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetSaleUpdates.sql:1-47`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedSaleData.sql:1-84`

[Verified] Legacy refresh: delete matching WWI Invoice IDs, then insert staged invoice lines. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedSaleData.sql:1-84`

[Verified] No own surrogate-key-0 row occurs in these fact snapshots; dimension references may use key 0 per lookup COALESCE. golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedSaleData.sql:1-84`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Fact.Sale.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedSaleData.sql:1-84`

### gold.fact_stock_holding

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Fact.Stock_Holding.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| stock_holding_key | int | BIGINT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:2` |
| stock_item_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:3` |
| quantity_on_hand | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:4` |
| bin_location | string max length=4 | NVARCHAR (20) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:5` |
| last_stocktake_quantity | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:6` |
| last_cost_price | decimal: max observed precision=6, scale=2, safe common decimal(6,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:7` |
| reorder_level | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:8` |
| target_stock_level | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:9` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:10` |


[Verified] Surrogate: stock_holding_key BIGINT IDENTITY(1,1). `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Stock Holding.sql:1-3`

[Inferred] Business grain/key: StockItemID per drop; source line identity remains in silver, with gold key mapping at published-row occurrence. SCD/update style: snapshot. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetStockHoldingUpdates.sql:1-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedStockHoldingData.sql:1-54`

[Verified] Legacy refresh: TRUNCATE and insert the entire holding snapshot; stock-item dimension selection orders Valid To descending. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedStockHoldingData.sql:1-54`

[Verified] No own surrogate-key-0 row occurs in these fact snapshots; dimension references may use key 0 per lookup COALESCE. golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedStockHoldingData.sql:1-54`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Fact.Stock_Holding.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedStockHoldingData.sql:1-54`

### gold.fact_transaction

[Verified] Exact ordered column contract: `golden/outputs/D2/tables/Fact.Transaction.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| transaction_key | int | BIGINT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:2` |
| date_key | date YYYY-MM-DD | DATE | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:3` |
| customer_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:4` |
| bill_to_customer_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:5` |
| supplier_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:6` |
| transaction_type_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:7` |
| payment_method_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:8` |
| wwi_customer_transaction_id | int | INT | observed NULLs=2468 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:9` |
| wwi_supplier_transaction_id | int | INT | observed NULLs=98664 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:10` |
| wwi_invoice_id | int | INT | observed NULLs=29464 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:11` |
| wwi_purchase_order_id | int | INT | observed NULLs=99034 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:12` |
| supplier_invoice_number | int | NVARCHAR (20) | observed NULLs=99034 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:13` |
| total_excluding_tax | decimal: max observed precision=9, scale=2, safe common decimal(9,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:14` |
| tax_amount | decimal: max observed precision=8, scale=2, safe common decimal(8,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:15` |
| total_including_tax | decimal: max observed precision=10, scale=2, safe common decimal(10,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:16` |
| outstanding_balance | decimal: max observed precision=9, scale=2, safe common decimal(9,2) | DECIMAL (18, 2) | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:17` |
| is_finalized | int; bit-compatible (not proof of SQL bit) | BIT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:18` |
| lineage_key | int | INT | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:19` |


[Verified] Surrogate: transaction_key BIGINT IDENTITY(1,1). `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Transaction.sql:1-3`

[Inferred] Business grain/key: source kind + CustomerTransactionID or SupplierTransactionID + extraction drop/occurrence; source line identity remains in silver, with gold key mapping at published-row occurrence. SCD/update style: append event. `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetTransactionUpdates.sql:1-58`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedTransactionData.sql:1-80`

[Verified] Legacy refresh: append all staged customer/supplier transaction extraction rows. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedTransactionData.sql:1-80`

[Verified] No own surrogate-key-0 row occurs in these fact snapshots; dimension references may use key 0 per lookup COALESCE. golden profile (`golden/outputs/<drop>/`); `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedTransactionData.sql:1-80`

[Verified] lineage_key belongs to the published contract; dimension default members use 0 and loaded rows use the entity load-attempt key. `golden/outputs/D2/tables/Fact.Transaction.csv:1`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedTransactionData.sql:1-80`

### gold.report_sale_by_day

[Verified] Exact ordered column contract: `golden/outputs/D2/reports/sale-by-day.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| invoice_date_key | date YYYY-MM-DD | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |
| sale_rows | int | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |
| total_including_tax | decimal: max observed precision=8, scale=2, safe common decimal(8,2) | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |


[Verified] Definition: `SELECT [Invoice Date Key],COUNT_BIG(*),SUM([Total Including Tax]) FROM Fact.Sale GROUP BY [Invoice Date Key]`; numeric aggregate equality holds in all drops; sums serialize at scale 2. Key/grain: invoice_date_key; no separate surrogate/SCD/default row. `golden/capture_sqlserver.py:40-45`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Implement as a view or refreshed materialized view over gold, with registered snake_case columns and matching result formatting. `golden/capture_sqlserver.py:40-49`; `src/parity/parity_check.py:127-133`

### gold.report_customer_history

[Verified] Exact ordered column contract: `golden/outputs/D2/reports/customer-history.csv:1`; observed D2 profile golden profile (`golden/outputs/<drop>/`)
| column (golden order) | observed type / exact format | legacy SQL type / expression | observed nullability | evidence |
| --- | --- | --- | --- | --- |
| customer_key | int | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |
| wwi_customer_id | int | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |
| customer | string max length=38 | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |
| valid_from | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |
| valid_to | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | derived SQL report expression | observed NULLs=0 | [Verified] golden profile (`golden/outputs/<drop>/`); `golden/capture_sqlserver.py:40-49` |


[Verified] Definition: `SELECT [Customer Key],[WWI Customer ID],Customer,[Valid From],[Valid To] FROM Dimension.Customer ORDER BY [Customer Key]`; exact multiset projection equality holds in D0/D1/D2. Key: customer_key; grain: dimension version; no separate surrogate/default row (the projection includes the dimension default). `golden/capture_sqlserver.py:46-49`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Implement as a view or refreshed materialized view over gold, with registered snake_case columns and matching result formatting. `golden/capture_sqlserver.py:40-49`; `src/parity/parity_check.py:127-133`

## §6 Integration outputs as platform controls

[Verified] Capture exports both Integration tables; registered parity comparison iterates only the manifest.tables business entries, not these exported files. `golden/capture_sqlserver.py:490-501`; `src/parity/parity_check.py:27-47`; golden profile (`golden/outputs/<drop>/`)

[Inferred] Retain logical equivalents as `control.etl_cutoff` and `control.lineage` (or job-run metadata plus compatibility views), not gold tables. Keep captured audit exports available; current parity does not require separate control-table publication. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLastETLCutoffTime.sql:10-19`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `src/parity/parity_check.py:27-47`

### control.etl_cutoff
| column (golden order) | observed type / format | NULL count | evidence |
| --- | --- | --- | --- |
| table_name | string max length=16 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| cutoff_time | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |


[Verified] Legacy key/DDL: `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Tables/ETL Cutoff.sql:1-4`; exact CSV header `golden/outputs/D2/tables/Integration.ETL_Cutoff.csv:1`

### control.lineage
| column (golden order) | observed type / format | NULL count | evidence |
| --- | --- | --- | --- |
| lineage_key | int | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| data_load_started | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| table_name | string max length=16 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| data_load_completed | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| was_successful | int; bit-compatible (not proof of SQL bit) | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| source_system_cutoff_time | datetime YYYY-MM-DD HH:MM:SS; fraction digits=[7] | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |


[Verified] Legacy key/DDL: `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Tables/Lineage.sql:1-8`; exact CSV header `golden/outputs/D2/tables/Integration.Lineage.csv:1`

[Verified] GetLineageKey creates a load-attempt record with start time, table name, requested cutoff, success=0 and completion NULL; MigrateStaged* marks success/completion and advances that table’s cutoff within its transaction. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:42-54`

[Inferred] Use explicit run/entity IDs and committed watermark state; advance each watermark only with successful entity publication, retain failure attempts and tie gold lineage values to the compatibility map. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:11-17`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:42-54`

## §7 Rationalization and in/out inventory
| legacy object | fate | evidence |
| --- | --- | --- |
| Integration.*_Staging + MigrateStaged*Data | [Inferred] Collapse transient transport into conformed silver plus one entity publication operation; SCD2 close+insert can be implemented using a staged MERGE pattern; retain parent replacement/append/snapshot semantics where defined, rather than treating every fact as update-in-place. | `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedCustomerData.sql:19-40`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedSaleData.sql:54-67`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedTransactionData.sql:54-63` |
| Get*Updates cursors, temp tables and temporal joins | [Inferred] Collapse into set-based silver transforms with captured temporal/as-of and cutoff semantics. | `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:37-69`; `L:wwi-ssdt/wwi-ssdt/Integration/Stored Procedures/GetCityUpdates.sql:166-176` |
| GetLineageKey / GetLastETLCutoffTime / ETL Cutoff / Lineage | [Inferred] Platform/control concern: run attempts, per-entity committed cutoffs and completion status; no separate business gold objects. | `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLastETLCutoffTime.sql:10-19` |
| Sequences and ReseedSequenceBeyondTableValues / ReseedAllSequences | [Inferred] Persisted key allocator/map concern; remove SQL Server sequence procedures, preserving published compatibility values. | `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Stored Procedures/ReseedSequenceBeyondTableValues.sql:23-32`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Stored Procedures/ReseedAllSequences.sql:1-15` |
| PopulateDateDimensionForYear | [Inferred] Deterministic silver.date generator and gold projection, retaining captured calendar/fiscal values. | `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/PopulateDateDimensionForYear.sql:9-19`; `golden/outputs/D2/tables/Dimension.Date.csv:1` |
| SQL Server columnstore rebuilds / index maintenance / partition maintenance | [Inferred] SQL Server statements disappear; any Delta layout, compaction and retention tuning becomes a platform concern, not a business transform. | `L:sqlserver/procedures/facts/Integration.usp_RebuildColumnstoreIndexes.sql:12-19`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Fact/Tables/Sale.sql:78-80`; `L:wwi-ssdt/wwi-ssdt/Application/Stored Procedures/Configuration_ApplyPartitioning.sql:1-227` |
| Agent statistics-refresh step / SSIS maintenance | [Inferred] SQL Agent/SSIS maintenance tasks disappear; statistics/layout policy belongs to the target platform. | `L:sqlserver/agent/10_job_WWI_Daily_ETL.sql:124-137` |
| SSIS logging and etl.usp_* batch/step/log procedures | [Inferred] Platform concern: job events, quality metrics, audit/run metadata; expanded branch logging is outside this captured entity model. | `L:sqlserver/procedures/facts/Integration.usp_RebuildColumnstoreIndexes.sql:50-58`; `golden/manifest.json:297-316` |
| SQL Agent WWI_Daily_ETL / DailyETLMain SSIS orchestration | [Inferred] Replace scheduling/dependencies with one Lakeflow job for the native scoped entity chain; expanded Master_Daily_ETL is not an additional business-output requirement. | `L:sqlserver/agent/10_job_WWI_Daily_ETL.sql:80-95`; `golden/capture_sqlserver.py:675-687` |
| Configuration_ReseedETL | [Inferred] Replace destructive warehouse reset with controlled bootstrap/replay operation plus default-member seeding, not a daily business table. | `L:wwi-dw-ssdt/wwi-dw-ssdt/Application/Stored Procedures/Configuration_ReseedETL.sql:8-35` |

| folder group | scope classification [Inferred] | evidence |
| --- | --- | --- |
| wwi-ssdt/ | [Inferred] In: Integration.Get*Updates and referenced OLTP/current/history schemas. Other operational application procedures are outside this model. | `L:wwi-ssdt/README.md:1-5`; `golden/capture_sqlserver.py:680-687` |
| wwi-dw-ssdt/ | [Inferred] In: Dimension/Fact/Integration entity DDL, migration procs, defaults and key definitions; other sample configurations are not additional outputs. | `L:wwi-dw-ssdt/README.md:1-5`; `golden/capture_sqlserver.py:490-501` |
| wwi-ssis/ | [Inferred] In: native DailyETLMain orchestration and mappings. | `L:wwi-ssis/README.md:1-3`; `L:wwi-ssis/wwi-ssis/DailyETLMain.dtsx:76-99` |
| sqlserver/ | [Inferred] Expanded SQL source/control/Agent/reference estate: consult for rationalization, not an extension of the captured 13 extraction entities. | `L:README.md:14-30`; `L:sqlserver/agent/10_job_WWI_Daily_ETL.sql:80-95` |
| ssis/ | [Inferred] Expanded orchestration/extract/quality/finance/customer/maintenance packages outside the native captured branch, except as platform design reference. | `L:README.md:14-30`; `golden/manifest.json:297-308` |
| oracle/, generators/, workload-drivers/, sample-scripts/ | [Inferred] Out: ERP definitions and synthetic/operational workload generation are not additional target outputs in this discovery. | `L:README.md:12-30`; `golden/capture_sqlserver.py:21-49` |
| wwi-app/, wwi-azure-functions/, wwi-ssasmd/, power-bi-dashboards/ | [Inferred] Out: application/service/cube/dashboard surfaces; only the two explicitly captured SQL reports enter this target model. | `L:README.md:3-16`; `golden/capture_sqlserver.py:40-49` |
| infrastructure/, deployment/, tools/, config/, docs/, validation/, parity/, .devin/ | [Inferred] Supporting deployment/tooling/documentation/validation inventory, not additional business target tables. | `L:README.md:25-30`; `L:README.md:54-59`; `golden/capture_sqlserver.py:490-515` |


[Verified] Folder names above were checked against the local repository root; classification uses the cited native capture dispatch and estate README, not an exhaustive analysis of every file in each folder. `L:README.md:12-30`; `golden/capture_sqlserver.py:21-49`

## §8 Per-drop output counts and transitions
| golden/outputs/<drop>/ file | D0 | D1 | D2 | D1−D0 | D2−D1 | evidence |
| --- | --- | --- | --- | --- | --- | --- |
| reports/customer-history.csv | 403 | 403 | 403 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| reports/sale-by-day.csv | 1069 | 1075 | 1081 | 6 | 6 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.City.csv | 116295 | 116295 | 116295 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.Customer.csv | 403 | 403 | 403 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.Date.csv | 1461 | 1461 | 1461 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.Employee.csv | 213 | 213 | 214 | 0 | 1 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.Payment_Method.csv | 6 | 6 | 6 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.Stock_Item.csv | 672 | 672 | 672 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.Supplier.csv | 28 | 28 | 28 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Dimension.Transaction_Type.csv | 15 | 15 | 15 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Fact.Movement.csv | 236667 | 238129 | 239983 | 1462 | 1854 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Fact.Order.csv | 231412 | 232838 | 234667 | 1426 | 1829 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Fact.Purchase.csv | 8367 | 8415 | 8471 | 48 | 56 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Fact.Sale.csv | 228265 | 229679 | 231486 | 1414 | 1807 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Fact.Stock_Holding.csv | 227 | 227 | 227 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Fact.Transaction.csv | 99585 | 100290 | 101132 | 705 | 842 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Integration.ETL_Cutoff.csv | 14 | 14 | 14 | 0 | 0 | [Verified] golden profile (`golden/outputs/<drop>/`) |
| tables/Integration.Lineage.csv | 13 | 26 | 39 | 13 | 13 | [Verified] golden profile (`golden/outputs/<drop>/`) |


[Verified] Dimensions are unchanged except Employee in D2: key 203 closes at `2016-06-14 08:00:00.0000000` and key 213 adds the next version for WWI employee 11. golden profile (`golden/outputs/<drop>/`); golden profile (`golden/outputs/<drop>/`)
| fact | transition | unchanged key rows | changed existing rows | new keys | deleted keys | columns changed on surviving keys | evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Movement | D0->D1 | 236667 | 0 | 1462 | 0 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Movement | D1->D2 | 238129 | 0 | 1854 | 0 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Order | D0->D1 | 231386 | 0 | 1452 | 26 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Order | D1->D2 | 232815 | 0 | 1852 | 23 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Purchase | D0->D1 | 8358 | 0 | 57 | 9 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Purchase | D1->D2 | 8358 | 0 | 113 | 57 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Sale | D0->D1 | 227981 | 0 | 1698 | 284 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Sale | D1->D2 | 229440 | 0 | 2046 | 239 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Stock Holding | D0->D1 | 0 | 227 | 0 | 0 | {'quantity_on_hand': 224, 'last_cost_price': 114, 'lineage_key': 227} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Stock Holding | D1->D2 | 0 | 227 | 0 | 0 | {'quantity_on_hand': 220, 'lineage_key': 227} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Transaction | D0->D1 | 99585 | 0 | 705 | 0 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |
| Transaction | D1->D2 | 100290 | 0 | 842 | 0 | {} | [Verified] golden profile (`golden/outputs/<drop>/`) |


[Verified] Stock_Holding remains a full 227-row snapshot each drop; migration truncates/reloads it and captured key values remain 1–227. Lineage grows by 13 rows each drop; ETL_Cutoff retains 14 entity rows: 13 entity cutoffs advance while Date remains at its initial cutoff. `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/MigrateStagedStockHoldingData.sql:28-37`; golden profile (`golden/outputs/<drop>/`); golden profile (`golden/outputs/<drop>/`); `golden/manifest.json:16-30`

[Verified] D0 cutoff baseline for the 13 extraction entities is `2016-06-06 12:31:17.0000000`; Date is `2012-12-31 00:00:00.0000000`. D1/D2 advance entity cutoffs to `2016-06-14 00:00:00.0000000` / `2016-06-21 00:00:00.0000000`. golden profile (`golden/outputs/<drop>/`); golden profile (`golden/outputs/<drop>/`); golden profile (`golden/outputs/<drop>/`)

## §9 Neutral implementation decisions for the lead

- [Inferred] Choose bootstrap packaging: retain D0 published key/value state alongside raw D0 inputs, then replay D1/D2. Decide whether bootstrap fixtures or a separate key-map dataset is the builder interface. `L:README.md:54-59`; golden profile (`golden/outputs/<drop>/`)

- [Inferred] Choose compatibility-column physical types versus separate typed silver fields; gold must expose the exact registered textual value contract and column ordering used by the current harness. `src/parity/parity_check.py:105-117`; `src/parity/parity_check.py:127-135`

- [Inferred] Decide view versus refreshed materialized view for the two derived reports and where operational control state lives (control schema versus job metadata views). `golden/capture_sqlserver.py:40-49`; `L:wwi-dw-ssdt/wwi-dw-ssdt/Integration/Stored Procedures/GetLineageKey.sql:11-20`

- [Inferred] Confirm the explicit owner and lifecycle of persisted surrogate/lineage compatibility maps, D0 bootstrap, replay checkpoints and diagnostic-copy retention before implementation. `L:wwi-dw-ssdt/wwi-dw-ssdt/Sequences/Stored Procedures/ReseedSequenceBeyondTableValues.sql:23-32`; `src/parity/parity_check.py:49-57`
