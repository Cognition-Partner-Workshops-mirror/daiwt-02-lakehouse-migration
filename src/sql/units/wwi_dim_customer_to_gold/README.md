# G02: Customer dimension

`wwi_dim_customer_to_gold` owns only `gold.dim_customer` and depends on
`p5_gold_seed` and `wwi_customer_to_silver`. Its `load` and `parity` tasks
accept the P2 drop parameters.

D0 restores the P5 customer baseline, including unknown key 0. D1/D2 read the
complete `silver.customer_changes` extract and the P5 Integration Customer
lineage row for the requested cutoff. The P3 SCD2 renderer closes open rows at
the minimum staged validity start, then inserts every staged version without
deduplication or attribute comparison. Validity strings retain datetime2(7)
precision. Keys continue after the target high-water mark, ordered by validity
start, customer ID, then the remaining stage columns to make ties deterministic.
Identical repeated rows remain repeated rows.

The per-drop `parity` task uses the shared row-for-row parity harness against
`Dimension.Customer.csv`. The supplied D0/D1/D2 snapshots each contain 403 rows;
incremental extracts are empty. Synthetic tests also exercise nonempty staging,
duplicate versions, unchanged attributes, key continuation and validity closure.
As with P3, replay after a partially applied drop requires restoring the
pre-drop state; D0 restores a clean baseline.
