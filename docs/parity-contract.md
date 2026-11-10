# Golden parity contract

[`src/parity/contract.json`](../src/parity/contract.json) derives from immutable
`golden/manifest.json`: **18 outputs per D0/D1/D2**, including two reports and
Integration Lineage / ETL_Cutoff. Entries specify table, drop, CSV path, ordered
columns, row count, byte size, SHA-256 and column comparators. Published CSVs live
under `/Volumes/daiwt_02_mig_prod/landing/golden/outputs/<drop>/`. Never edit
repository or volume goldens to fix parity.

## Comparison and formatting

[`parity_check.py`](../src/parity/parity_check.py) verifies each compared file's
byte size and SHA-256 via SDK Files, then uses warehouse `read_files` and
**EXCEPT ALL in both directions**. Duplicates count; row order does not. Golden
row count must also match the contract. There is no sampling, tolerance or
deduplication. Missing, extra and per-column samples diagnose failures; samples
are not the acceptance check.

CSV uses UTF-8, headers, LF, commas and double-quote escaping; `\N` means SQL NULL,
distinct from an empty string. Headers are snake_case; `source_columns` preserves
legacy names. Decimal scale, dates, bit `0`/`1`, spatial WKT and uppercase binary
hex must retain captured spelling.

Datetime2(7) requires seven fractional digits. Bronze keeps `_str` companions
alongside typed timestamps; use lossless text for seventh-digit values and temporal
boundaries. Parity's timestamp renderer formats six digits plus trailing `0`; it
cannot recover precision lost by a cast. P5 keeps datetime columns as strings.

Only contract-defined nondeterministic Lineage load timestamps are normalized:
`data_load_started` must be non-null valid datetime7; `data_load_completed` must be
valid and greater than or equal to start. Invalid values fail even if both sides
are invalid. Lineage keys, source cutoffs and other strict columns compare exactly;
these are not blanket ignored columns.

S05 has a local **D0-only `tax_rate` waiver** in
[`wwi_stock_item_to_silver/parity.sql`](../src/sql/units/wwi_stock_item_to_silver/parity.sql):
the restored D0 dimension's tax rate is not derived from D0 inputs. This does not
waive D1/D2 or the central Gold contract (`gold.dim_stock_item.tax_rate` is strict).

## Drop and producer scope

```bash
python src/parity/parity_check.py --suffix pr_123 --drop D0 \
  --units out/selected-units.json --output out/parity-D0.json
```

`--drop` accepts D0/D1/D2 (default D2). `--units` takes a JSON list of registered
unit names, not a comma-separated CLI list. Unknown names or invalid JSON fail.

| Output state | Result |
| --- | --- |
| No registered producer | SKIP: `not yet migrated` |
| Producer outside supplied `--units` | SKIP: `producer not selected` |
| Selected producer, missing table or inaccessible/mismatched golden | FAIL |
| Registered output matching the comparison | PASS |

Without `--units`, all registered outputs compare (including production callers).
Skipped outputs are neither downloaded nor queried. Dev schemas resolve to
`<layer>_pr_<n>`; prod uses fixed layers. Exceptions fail closed and the CLI writes
FAIL JSON. An all-SKIP success is explicitly labelled **not parity evidence**,
not proof of migrated data correctness. See [CI flow](ci-flow.md) for evidence.
