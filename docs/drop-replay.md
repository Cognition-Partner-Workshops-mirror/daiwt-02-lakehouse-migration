# Drop replay

## Immutable inputs

`golden/` and `/Volumes/daiwt_02_mig_prod/landing/golden/` are authoritative
captures, never repair targets. The manifest describes files, hashes, row counts,
drop cutoffs and deltas. When legacy SQL text and captured results disagree,
match golden; do not regenerate or edit golden files.

The supported CI sequence is **D0 -> D1 -> D2**. D0 is the restored full baseline;
D1/D2 inputs are incremental upserts with `*.deleted.csv` companions. Output CSVs
describe each drop's resulting state, not just its input delta. `ci/drops.py`
reads `golden/manifest.json` and supplies `drop`, `last_cutoff`, `new_cutoff` job
parameters. Cutoffs are fixed-width datetime2(7) strings, not wall-clock values;
entity cutoff lists must agree, excluding Date.

```bash
python ci/drops.py D1
```

## Bronze landing (P4)

[`bronze_landing_replay.py`](../src/bronze/bronze_landing_replay.py) uses
[`tables.json`](../src/bronze/tables.json) for 38 inputs (25 base, 13 archive).
It verifies input checksums, CSV headers and row widths.

- Raw `<table>_raw` rows preserve STRING values with `_drop`, `_op`, `_file`,
  `_ingested_at` metadata. D0 resets replay state; later drops append raw data.
- Typed `<table>` snapshots are replaced at D0. Later drops delete first, then
  upsert. Primary-key tables use keys; keyless tables retain whole-row multiset
  semantics, including duplicate counts.
- `TIMESTAMP_NTZ` projections retain original datetime2(7) text in `<column>_str`
  companions. Snapshot verification uses these strings without losing digit seven.
- Each snapshot is rendered back to golden CSV order/spelling and checked against
  manifest `full_row_count` and `full_sha256`. `golden_drops` and
  `bronze_replay_log` retain replay metadata/results.

D1/D2 require the preceding drop fully applied. A drop already started cannot
simply be replayed in place: restart from D0. Do not bypass guards or invent cutoffs.

## D0 gold seed (P5)

[`p5_gold_seed`](../src/sql/units/p5_gold_seed/README.md) restores all 16
non-report baseline contract tables at D0 with `replace=True`. The reusable
`seed()` helper otherwise preserves existing tables unless replacement is explicit.
Unknown members and key-0 references are copied, not synthesized. Datetime, spatial
WKT and hex-photo strings remain lossless; no lineage row 0 is invented. Date stays
the captured 1,461-row 2013–2016 baseline, without year extension.

P5 registers only `gold.integration_lineage` / `gold.integration_etl_cutoff`;
Gold units own their seeded dimension/fact outputs. D1/D2 use Integration control
to reserve the 13-entity lineage block, complete it and advance cutoffs (not Date).
There must be one run-start writer per schema. See the P5 guide for retry behavior
and [docs/daily-etl.md (P8)](daily-etl.md) for DAG integration.

## Quality without data loss (P9)

[Governance helpers](governance.md) use WARN expectations and quarantine **copies**,
never drop/fail expectations or silent business-row filtering. `quarantine_sql()`
replaces diagnostic rows for the requested `_drop`, adds `_failed_rules` and
`_quarantined_at`, and never mutates the source. NULL rules fail. Serialize each
quarantine table's DELETE/INSERT sequence; retry interrupted runs to completion.
Quarantine must be empty for golden inputs. Register source and diagnostic tables
in the appropriate unit's `owns`, without re-owning bronze inputs.
