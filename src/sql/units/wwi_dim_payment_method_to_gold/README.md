# G05 Payment Method dimension

Unit and job: `wwi_dim_payment_method_to_gold`; publishes only
`gold.dim_payment_method`. Depends on `p5_gold_seed` and
`wwi_payment_method_to_silver`.

D0 replaces the dimension with P5's D0 golden seed, including key 0 Unknown.
D1/D2 read exactly one Payment Method lineage key for new_cutoff from P5's
Integration lineage table; Integration tables are never modified here.
An existing target row with that lineage skips the load on rerun.

The load projects the Silver changes snapshot to the seeded BIGINT/STRING types,
preserving datetime2(7) precision. Shared SCD2 SQL closes open versions at the
minimum staged valid_from, then inserts every staged version with HWM + ROW_NUMBER
keys ordered by valid_from, business ID, and payment method. Empty staging
naturally changes nothing; unchanged staged values are not suppressed.
As with the shared SCD2 framework, partial failures require restoring pre-drop
state before replay; the lineage guard is not a transaction/retry mechanism.

Job parameters are drop, last_cutoff, and new_cutoff; both cutoffs are validated.
Notebook parameters catalog, gold_schema, silver_schema, and golden_path come
from bundle variables. The dependent parity task compares the run drop's golden
CSV through the shared comparator and raises on non-PASS.

Local tests: `python -m pytest -q src/sql/units/wwi_dim_payment_method_to_gold/test_dim_payment_method.py`.
