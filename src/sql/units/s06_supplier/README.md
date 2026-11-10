# S06 Supplier

Matches legacy output: SupplierCategories and Suppliers (base plus archive) drive
events; People is an AS-OF inner join only. Each event emits the supplier's own
ValidFrom, preserves ties, and recomputes ValidTo from the next strictly greater
staged start. No no-op suppression or event deduplication is performed.

## Platform integration

P4's `bronze_landing_replay` owns and replays the shared bronze tables.
The registry dependencies order P4 and P6 before S06 when selected together.
S06 does not ingest or own any shared source or raw tables.

The job accepts `drop`, `last_cutoff`, `new_cutoff` from `ci/run_units.py`.
`ci/dev_verify.sh pr_18 ''` classifies the changed registry entry and includes
its P4/P6 prerequisites. It cleans owned objects, then runs D0, D1, D2 in order.
Each S06 run writes its one-row `silver.s06_supplier_run_window`, refreshes the
pipeline and checks parity.
The pipeline configuration is immutable during runs.

`logic.change_sql` reads P6's silver `purchasing_suppliers_history`,
`purchasing_supplier_categories_history`, and `application_people_history` views.
Their `valid_from` / `valid_to` strings derive from P4's exact seven-digit
**ValidFrom_str / ValidTo_str**, never the microsecond TIMESTAMP_NTZ columns.
Point-in-time joins use the P6 half-open predicate `valid_from <= t AND t < valid_to`.
P6's `temporal_change_events` supplies windowed source/instant pairs; joining
them back to each driving history restores source-row multiplicity before the
legacy change walk. People is never selected as a driver, and supplier/category
events at the same instant remain separate.

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
