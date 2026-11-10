# G03 static Date dimension

Registry unit and job: `wwi_dim_date_to_gold`; depends on `p5_gold_seed`.
Publishes only `gold.dim_date` and creates no control tables.

The `seed` task calls P5's seed helper with `tables=["gold.dim_date"]` and
`replace=True` on every drop. This restores exactly the 14 D0 golden columns
and 1,461 days (2013-01-01 through 2016-12-31), including fiscal attributes.
There is no calendar generation, year extension, or Integration cutoff update.
Repeated runs and D0 → D1 → D2 runs all restore the same static contents.

The dependent `parity` notebook uses the shared contract comparator against
`outputs/<drop>/tables/Dimension.Date.csv`, including row counts and bidirectional
`EXCEPT ALL`; a mismatch fails the task. No parity results table is created.

Job parameters are `drop`, `last_cutoff`, and `new_cutoff`. Only `drop` selects
parity's golden output; the cutoff parameters are accepted for runner compatibility
and do not alter the static dimension. Notebook parameters `catalog`, `gold_schema`,
and `golden_path` come from bundle variables.

Local tests: `PYTHONPATH=src:ci python -m pytest -q src/sql/units/wwi_dim_date_to_gold/test_dim_date.py`.
