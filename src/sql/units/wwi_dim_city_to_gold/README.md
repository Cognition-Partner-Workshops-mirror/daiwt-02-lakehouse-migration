# G01 — City dimension

The load task restores D0 with the P5 seed (`replace=True`), including key 0.
D1/D2 read only the current `_drop` from S01's `silver.city_changes`.
P3 closes open versions at the minimum staged valid_from and inserts every
staged occurrence in `_staged_order`, allocating keys after MAX(city_key).
S01 supplies the legacy validity end: the next strictly later valid_from for
the city, or end-of-time. Fixed-width datetime2(7) strings and location text
are retained without timestamp or geography conversion.

Lineage is the unique City row in P5's `gold.integration_lineage` selected
by `new_cutoff`; P5 owns allocation, completion and cutoff advancement.
The parity task compares all columns and occurrences against the selected
drop's Dimension.City.csv using the shared parity harness.

Replay D0 → D1 → D2 to reset the baseline before incremental loads, as the
CI runner does. A partially executed incremental load must be recovered by
replaying from D0; the two P3 statements are not a transaction.

Local coverage: `.venv/bin/pytest -q`.
