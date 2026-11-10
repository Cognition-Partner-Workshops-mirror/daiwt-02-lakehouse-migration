# G11 — Purchase fact

Registry unit: `wwi_fact_purchase_to_gold`; output: `gold.fact_purchase`.
Depends on P5 run control / baseline, purchase silver staging, and the supplier
and stock-item Gold dimensions.

D0 restores only the purchase baseline using the P5 seed. D1/D2 select the
requested `_staging_drop`; staging accumulates across drops per spec.
The P3 range lookup uses last-modified datetime2(7) text, strict lower and
inclusive upper validity bounds, and unknown key 0 for missing dimensions.

The high-water mark is captured before deleting all staged purchase-order IDs.
Every staged occurrence is inserted in `_drop, _staged_order` order, preserving
the upstream purchase-order / line ordering. D2 reapplies D1 rows before D2 rows.
P5 owns cutoff advancement; lineage uses the Purchase allocation in its run block.
Source staging and dimensions must be immutable while this single-writer job runs.
Replay requires restoring D0 and applying windows in order; incremental writes
are not retry-idempotent.

The `parity` task compares the requested drop row-for-row against its golden
snapshot. Expected D1: 8,358 baseline rows plus 57 lineage-23 rows (8368–8424).
Expected D2: 8,358 baseline rows plus 113 lineage-36 rows (8425–8537).
No golden files are modified.
