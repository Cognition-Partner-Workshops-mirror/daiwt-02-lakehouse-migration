# G12 — Sale

Translation of `Integration.MigrateStagedSaleData` to `gold.fact_sale`.
D0 restores the P5 golden baseline. D1 and D2 read S11's
`silver.sale_changes`, which holds the latest replayed drop only, so the job
first requires the latest `bronze_replay_log` drop to be the requested drop.

Staging order is WWI Invoice ID, then source invoice line. S11 keeps the native
positional columns without the line ID, so each staged row is matched back to
its `bronze.sales_invoice_lines` row on invoice, stock item and line values
(identical lines pair by occurrence). Every staged row must match a line.

Dimension keys use the P3 lookup on `Last Modified When` rendered as
datetime2(7) text: `natural_id =`, `ts > valid_from AND ts <= valid_to`,
earliest `valid_from` then key, default 0. City, Customer, Bill To Customer,
Stock Item and Salesperson (Employee) are looked up this way. Undelivered
invoices keep a NULL delivery date. Decimals are cast through STRING.

The sale key high-water mark is read before rows are deleted. All fact rows
for staged WWI Invoice IDs are deleted, then staged rows are inserted with
keys HWM + 1.. in staging order. P5 Integration reserves the Sale lineage
key and marks it complete only after the insert succeeds.
The parity notebook uses the shared contract and parity harness for exact
row-for-row comparison on the requested D0, D1 or D2 drop.

Local checks:

```sh
pytest -q tests src/sql/units/wwi_fact_sale_to_gold
```
