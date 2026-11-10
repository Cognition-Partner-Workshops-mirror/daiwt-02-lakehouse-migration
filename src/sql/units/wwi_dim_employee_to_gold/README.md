# G04 Employee dimension

Unit and job: `wwi_dim_employee_to_gold`; publishes only `gold.dim_employee`.
Depends on `p5_gold_seed` and `wwi_employee_to_silver`.

D0 replaces the dimension with P5's D0 golden seed, including key 0 Unknown.
D1/D2 read exactly one Employee lineage key for new_cutoff from P5's
Integration lineage table; Integration tables are never modified here.
An existing target row with that lineage skips the load on rerun.

The load projects `silver.employee_changes` to the seeded BIGINT/STRING types,
preserving datetime2(7) precision; photo stays uppercase hex text. Shared SCD2
SQL closes open versions at the minimum staged valid_from (the closed row keeps
its lineage), then inserts every staged version with HWM + ROW_NUMBER keys
ordered by valid_from, business ID and the remaining attributes. Empty staging
changes nothing; unchanged staged values are not suppressed. Partial failures
require restoring pre-drop state before replay.

Job parameters are drop, last_cutoff and new_cutoff; both cutoffs are validated.
Notebook parameters catalog, gold_schema, silver_schema and golden_path come from
bundle variables. The dependent parity task compares the run drop's golden CSV
through the shared comparator and raises on non-PASS.

Local tests: `python -m pytest -q src/sql/units/wwi_dim_employee_to_gold/test_dim_employee.py`.
