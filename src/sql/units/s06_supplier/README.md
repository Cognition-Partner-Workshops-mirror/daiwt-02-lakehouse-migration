# S06 Supplier

Matches legacy output: SupplierCategories and Suppliers (base plus archive) drive
events; People is an AS-OF inner join only. Each event emits the supplier's own
ValidFrom, preserves ties, and recomputes ValidTo from the next strictly greater
staged start. No no-op suppression or event deduplication is performed.

## Platform integration

P4's `bronze_landing_replay` owns and replays the shared bronze tables.
The registry dependency makes the merged runner execute it before S06 on every
drop. S06 does not ingest or own any shared source or raw tables.

The job accepts `drop`, `last_cutoff`, `new_cutoff` from `ci/run_units.py`.
`ci/dev_verify.sh pr_18 s06_supplier` selects S06 and its dependency closure,
cleans owned objects, then runs D0, D1, D2 in order. Each S06 run writes its
one-row `bronze.s06_supplier_run_window`, refreshes the pipeline and checks parity.
The pipeline configuration is immutable during runs.

Until P6 is merged, `logic.change_sql` supplies unit-local CTEs named
`purchasing_suppliers_history`, `purchasing_supplier_categories_history`,
and `application_people_history`, matching draft PR #25's history names.
They union P4's base/archive snapshots and expose `valid_from` / `valid_to`
from **ValidFrom_str / ValidTo_str**, never the microsecond TIMESTAMP_NTZ columns.
Their AS-OF predicate is `valid_from <= t AND t < valid_to`.
Replace those CTEs with P6's silver history views when available; do not use
its distinct timestamp change-event helper because S06 preserves event ties.

## Strict silver parity

P1 only compares registered manifest outputs; its contract contains no silver
staging outputs. S06 therefore retains its strict per-drop parity task.
D0 is the restored baseline (00-units P5), not an extract: last_cutoff equals
new_cutoff, so its staged set must be empty. D1/D2 expected staging is the positive
multiset difference of consecutive Dimension.Supplier snapshots, projected to the
nine native result columns. All three expected stage counts are **0**; all three
golden dimension snapshots contain **28** rows, including the unknown member.
Quarantine must be empty for each drop. Comparisons retain all seven fractional
digits and render NULL as `\N`. A difference raises SQL `raise_error`.

Local SQL tests require PySpark 3.5 and JDK 17. They deliberately supply incorrect
timestamp-column values to prove that only the seven-digit string companions
drive the extraction.
