# G13 — Stock Holding

Translation of the full Stock Holding reload to `gold.fact_stock_holding`.
D0 restores the P5 golden baseline, just as G06 does. D1 and D2 select only
the requested drop from S12's cumulative `silver.stock_holding_snapshot`.
The presence of that drop is required before replacing the fact; S12 has no
separate window table. Cutoffs are validated but do not filter this snapshot.

Each reload assigns keys starting at 1 in ascending WWI Stock Item ID order.
All 227 golden rows in each drop map to this order through the golden Stock
Item dimension. Dimension lookup chooses the latest valid_to without a time
window, defaulting to key 0. Last cost price is decimal(18,2), via a string
cast to preserve printed decimal rounding.

P5 Integration reserves the Stock Holding lineage key and marks it complete
only after the replacement succeeds. D0 retains baseline lineage from seed.
The parity notebook uses the shared contract and parity harness for exact
row-for-row comparison on the requested D0, D1 or D2 drop.

Local checks:

```sh
pytest -q tests src/sql/units/wwi_fact_stock_holding_to_gold
```
