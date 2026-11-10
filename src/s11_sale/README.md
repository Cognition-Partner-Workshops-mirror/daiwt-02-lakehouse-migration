# S11 — Sale silver

P2 runs `wwi_sale_to_silver` once per drop with `drop`, `last_cutoff`,
and `new_cutoff`. P4's `bronze_landing_replay` must finish first.
The job validates P4 completion/window, refreshes the declarative pipeline,
then performs strict Sale parity. It never writes bronze or gold.

## Shared bronze and window

Read P4 typed snapshots `sales_invoices`, `sales_invoice_lines`,
`warehouse_stock_items`, `warehouse_package_types`, and `sales_customers`.
Column names retain the source spelling; IsChillerStock is BOOLEAN, amounts
are fixed-scale DECIMAL, datetime2 values are TIMESTAMP_NTZ.
P4 owns casting and raw landing. S11 validates required keys/fields and FKs.

The pipeline reads the latest drop index from `bronze_replay_log` and the
Sale window from `golden_drops`. Prepare and parity both require every
manifest input to have a PASS record for the requested drop and matching
Sale cutoffs. No unit-owned context table is needed. Do not run P4 or the
pipeline independently while another replay is active.

GetSaleUpdates uses current-state joins, not temporal AS-OF joins.
P6 archives/views are not needed. The modification window is (last, new].
The output follows all 20 native positional columns, including
`WWI Salesperson ID`. Excluding tax is ExtendedPrice - TaxAmount.
Consumers must explicitly order Delta reads; query ordering is not a storage
guarantee.

Expectations warn only; invalid candidates remain in sale_changes and are
also copied to sale_changes_quarantine. Source rows with missing primary
keys are quarantined too. P4 cast failures fail its job before S11.

## Strict parity and ownership

D0 has equal Sale cutoffs and must yield zero stage and quarantine rows.
D1/D2 compare against golden Fact.Sale rows with sale_keys absent from the
previous drop. Compare the 14 shared native fact fields plus five resolvable
natural IDs, as golden CSV strings with fixed decimal scales and ISO dates.
Dimension IDs resolve using golden dimension CSVs and the legacy DW interval
(valid_from, valid_to]; unknown key-0 members project to NULL.
Counts must match, both EXCEPT ALL directions must be empty, and quarantine
must be empty. Each job prints/returns per-drop JSON and fails on any mismatch.

P0/P1's global contract covers 18 golden destination outputs, not Sale staging;
strict silver parity therefore stays inside this job. Registry outputs is [].
Ownership is only silver.sale_changes and silver.sale_changes_quarantine;
depends_on is bronze_landing_replay. G12 owns gold.fact_sale.

## Verification

```sh
python -m pytest -q -s
databricks bundle validate --strict -t dev --var schema_suffix=pr_20
ci/dev_verify.sh pr_20 'bronze_landing_replay,wwi_sale_to_silver'
```

Verification must run on a clean, pushed exact head for P2 attestation.
Local Spark tests use P4's typed projection for D0/D1/D2 golden replay.
