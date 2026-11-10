# G07 — Supplier dimension

Registry unit: `wwi_dim_supplier_to_gold`; output: `gold.dim_supplier`.
Depends on P5 run control / baseline and S06 `silver.supplier_changes`.

D0 restores only the supplier baseline using the P5 seed. D1/D2 apply the
P3 SCD2 renderer: close current rows at the minimum staged start, then insert
every staged version. Keys continue from the seeded high-water mark in ascending
stage-column order (supplier ID, attributes, validity). Identical stage rows
remain distinct occurrences. Source changes are immutable during the job;
upstream S06 finishes before this single-writer job runs.

Lineage uses the Supplier allocation in P5's ordered run block. P5 owns cutoff
advancement. Replay requires restoring D0 and applying windows in order; the
incremental mutation is not retry-idempotent for nonempty change sets.

The visible `parity` task compares the requested drop against its golden snapshot
with the shared row-for-row parity harness. No golden files are modified.
