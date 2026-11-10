# G10 — Order

Translates Integration.MigrateStagedOrderData to gold.fact_order.
D0 restores only the P5 Fact.Order seed. D1/D2 read the matching S09
order_extract_window and order_changes drop. Empty matching windows are valid.

Five P3 time-range lookups use exact datetime2(7) text, earliest valid_from
then key, with fallback 0. The target high-water mark is read before deleting
all rows for staged WWI Order IDs. Inserts allocate keys in _stage_seq order,
cast all 19 contract columns to P5 seed types, and retain nullable dates/IDs.
P5 Order lineage is completed only after successful insertion.

Run offline checks: `pytest -q tests src/sql`. The job runs load then strict
per-drop parity. Replays must restore the previous drop before retrying;
this delete/insert loader does not provide transactional retry guarantees.
