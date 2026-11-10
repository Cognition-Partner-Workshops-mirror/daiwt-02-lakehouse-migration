# P5 baseline and Integration control

`seed(spark, contract, catalog, schema, golden_path, tables=None, replace=False)`
is the reusable D0 step. G-units can pass their `gold.<table>` names in `tables`;
P8 can seed all 16 baseline tables before its DAG. Existing tables are preserved
unless `replace=True` explicitly restores D0. The P5 D0 job restores all 16.
Only the two Integration outputs are registered to P5; G-units own their seeded
dimension/fact tables. Reports are not seeded.

Date is copied verbatim (14 columns, 1,461 rows, 2013–2016); no year extension.
Unknown members and lineage key 0 references are copied, not synthesized.
Datetime2(7), spatial WKT and hex photo values remain lossless strings; date,
integer and decimal columns have native types. No lineage row 0 is created.

`Integration(spark, catalog, schema).begin_run(new_cutoff)` reserves the full
13-key block in legacy order and returns `{entity: key}`. Call once in P8's
run-start task, before parallel G-unit work. `start_lineage(entity, cutoff)`
returns the same reserved key. `complete_lineage(key)` marks an entity successful;
run-end calls `advance_cutoffs(new_cutoff)` for all 13 entities, even with empty
staging. `get_cutoff(entity)` reads control state. Date is never advanced.
The standalone P5 job completes the block without transforming dims/facts.

There must be only one run-start writer per schema. Retries at the same cutoff
reuse the block and preserve completed timestamps. D1/D2 require preceding
cutoffs and accept their own completed cutoff for retries. Job parameters come
from the runner's manifest; wall clock is used only for load timestamps.

Before replacing/deleting gold rows, call `high_water_mark(spark, table, key)`
on the seeded table and `allocate_keys(hwm, count)`. The returned range starts
at `hwm + 1`, including for zero-row stages; retain the returned HWM in caller
run state so deletions cannot recycle allocated keys. P3's SCD2 renderer accepts
this HWM. These helpers assume a single writer; they are not concurrent sequences.
