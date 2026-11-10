# S06 Supplier

Matches legacy output: SupplierCategories and Suppliers (base plus archive) drive
events; People is an AS-OF inner join only. Each event emits the supplier's own
ValidFrom, preserves ties, and recomputes ValidTo from the next strictly greater
staged start. Temporal values stay datetime2(7) CSV strings, avoiding Spark's
microsecond truncation. NULL is loaded as the CSV sentinel `\N` in bronze.

## Replay and parity

The registered job runs D0 bronze → pipeline → strict parity, then D1, then D2.
It is directly runnable by the existing `ci/run_units.py` / `ci/dev_verify.sh`;
no shared runner edits are needed. Parity raises SQL `raise_error` on any multiset
difference or nonempty quarantine. The job fails before advancing to the next drop.

D0 is the restored baseline (00-units P5), not an extract: last_cutoff equals
new_cutoff, so its staged set must be empty. D1/D2 expected staging is the positive
multiset difference of consecutive Dimension.Supplier snapshots, projected to the
nine native result columns. All three expected stage counts are **0**; all three
golden dimension snapshots contain **28** rows, including the unknown member.
Quarantine must be empty for each drop. Comparisons retain all seven fractional
digits and compare `\N` literally.

## Scoped platform substitutes

`bronze.py` reconstructs current state from D0 through the selected drop, applying
deletes before upserts, preserving archive multiplicity. It writes the documented
six bronze names plus six unit-prefixed raw operation tables with ingest metadata.
It is deliberately deterministic on rerun. The raw tables are reconstructed from
immutable drops; they are not a long-lived append log. Replace this task with P4
when available. People names intentionally match the shared contract; do not run
this substitute concurrently with another writer of those tables.

`logic.change_sql` is the P6 stand-in: base UNION ALL archive with `[from,to)`
string comparisons. Swap its history CTEs for P6 views when available. `depends_on`
is empty until those platform units have registry keys; owns lists every table.

One pipeline owns the two silver outputs. Before each update the bronze task
overwrites the one-row `bronze.s06_supplier_run_window` control table with `drop`,
`last_cutoff`, and `new_cutoff`; the pipeline reads that table without modifying
its deployed configuration. The job refreshes it and checks parity. Runs are serialized. No deployment is needed for
the offline tests. Local SQL test requires PySpark 3.5 and JDK 17.
