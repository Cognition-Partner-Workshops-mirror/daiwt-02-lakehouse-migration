# G06 Stock Item dimension

`wwi_dim_stock_item_to_gold` publishes `gold.dim_stock_item` after
`p5_gold_seed` and `wwi_stock_item_to_silver`. The runner executes these
dependencies; the job contains only `load` and dependent `parity` tasks.

D0 replaces the dimension with the 672-row P5 baseline, including unknown key 0,
and clears replay state. D1/D2 require the matching silver window and select
changes with valid_from > last_cutoff and valid_from <= new_cutoff.
P3 SCD2 closes open members at each business key's earliest staged validity
start and inserts every staged version. Keys follow valid_from, stock item ID,
then the remaining columns. Decimal types match the seed, datetime2(7) remains
fixed-width text, and binary photos become uppercase hex with NULL preserved.
Integration supplies lineage keys; this unit does not complete lineage or
advance cutoffs.

The dimension's `wwi.stock_item.checkpoint` Delta property stores each drop's
pre-load version, original and reserved key high-water marks, stage count,
window and completion flag. Retries restore that version and reapply the
allocation. D2 requires completed D1 with a contiguous window; rerunning D1
after D2 requires D0 first. Keep Delta history available for RESTORE and run
only one writer per dimension/schema. Silver inputs must remain immutable
through retries. D0 intentionally resets all replay checkpoints.

Parity compares the run drop's golden output using the shared comparator and
fails on a non-PASS result. No additional output/control table is created.

Local tests: `PYTHONPATH=src:ci python -m pytest -q src/sql/units/wwi_dim_stock_item_to_gold/test_dim_stock_item.py`.
