# SQL Server golden capture

Requires Python, `pymssql`, `boto3`, AWS permission to read the
`daiwt-02-sqlserver` secret in `us-east-1`, and network access to that server.
No SSIS runtime is needed. The legacy checkout is used only for its HEAD SHA
in provenance, never imported. Its default location is
`~/repos/daiwt-02-enterprise-etl`.

Read-only commands, from the repository root:

```sh
python golden/capture_sqlserver.py export --drop D0 --out golden
python golden/capture_sqlserver.py manifest --out golden
python golden/capture_sqlserver.py verify --out golden
```

For generated drops, capture inputs before ETL and outputs afterwards:

```sh
python golden/capture_sqlserver.py export --drop D1 --phase inputs --out golden
python golden/capture_sqlserver.py etl --new-cutoff '2016-06-14 00:00:00'
python golden/capture_sqlserver.py export --drop D1 --phase outputs --out golden
```

Inputs-only capture writes a pending sidecar; outputs publishes the completed
drop sidecar. Only completed drops enter the manifest. `advance` prints window
activity counts and fails on zero activity. `etl` prints per-entity extracted
counts and fails if all incremental extracts are empty (StockHolding's full
snapshot is excluded from that check). Unchanged individual dimensions can
legitimately return zero rows.

`export` refuses to overwrite a drop, including an incomplete one. Use a fresh
output directory after investigating a failed export. Files are written via
`.partial` files and a completed `capture-D0.json` sidecar is published last.
`manifest` accepts available D0..Dn drops and points the parity contract
at the last available drop. It checks captured file hashes before publication.
`verify` checks the CSV inventory, hashes, headers, widths and row counts, then
compares last-drop output table counts (including Integration tables) to live
DW counts. Reports are verified against their captured hashes/counts, not
recomputed. Counts and exports are not a transactionally consistent snapshot;
keep the estate idle during capture. Cutoff changes during export cause failure.

D0 inputs are full. Later inputs contain inserted or changed whole rows compared
by primary key to the previous completed drop's full export. Unchanged tables
still get a header-only CSV. Disappeared primary keys are written to
`<db.schema.table>.deleted.csv` (PK columns only); no deletion file is written
if there are no deletions. Outputs and reports remain full every drop.

Full source exports live outside the repo at
`~/golden-work/full/<drop>/<db.schema.table>.csv`, including D0. Keep these files
between drops. Existing scratch files are not overwritten. Delta export requires
the preceding completed capture sidecar and matching full scratch hashes; it
fails if they are missing or changed. A new estate/reset capture needs a fresh
scratch directory (move the old directory aside first).

Tables without declared PKs use whole-row multiset identity instead: changed
rows become a removal plus an insertion. Their deletion CSV contains all columns,
including repeated rows when needed to preserve duplicates. No synthetic PK is
invented. SQLite indexes in temporary directories under `~/golden-work` bound
memory usage. The manifest records `primary_key`, `key_columns`, `key_semantics`,
`previous_input`, `input_kind`, `full_path`, `full_row_count`, and `full_sha256`
alongside each delta's own columns, row count and SHA256.

`verify` reconstructs each available full snapshot from the D0 base plus the
chained deletes/upserts. It compares complete rows and multiplicities against
scratch, serializes reconstructed rows in the scratch export's SQL Server order,
and checks the resulting full CSV SHA256 and row count. This avoids imposing
Python/SQLite ordering on SQL Server keys/collations. Missing scratch is reported
as `RECONSTRUCT SKIP`, without skipping ordinary delta-file hash/count checks.
Old full-only manifests without scratch metadata are also explicitly skipped.

Temporal source tables' engine-managed history tables are included too, even
when their names appear only through FOR SYSTEM_TIME rather than explicit SQL.
Primary-key tables use 50,000-row keyset pages, preserving seven-digit timestamp
keys as SQL-formatted strings. Tables without PKs stream a single query ordered
by all columns, using text representations for non-orderable spatial/LOB types.
Output/report headers are snake_case for the parity harness's SQL identifier
contract; manifest `source_columns` preserves the original ordered SQL names.
Inputs retain native SQL column names. Values are formatted on SQL Server, not
by Python. A literal source string `\N` is indistinguishable from NULL under the
existing parity contract. Nondeterministic columns are recorded, not excluded.

These commands **mutate** the estate. Do not run without approval:

```sh
python golden/capture_sqlserver.py advance --start 2016-06-01 --end 2016-06-02
python golden/capture_sqlserver.py etl --new-cutoff '2016-06-03 00:00:00'
```

`advance` follows the live PopulateDataToCurrentDate wrapper: configuration
apply, DailyProcessToCreateHistory with the explicit window, configuration
remove. It never calls PopulateDataToCurrentDate. Values follow the legacy
`wwi-ssdt/README.md` example: orders/day 60, Saturday 25%, Sunday 0%, silent 0,
dates printed 1. UpdateCustomFields=0 matches the wrapper. Temporal deactivation,
reactivation, sequence reseeding and RLS reapplication already occur inside the
simulation procedures. On failure it does not attempt speculative cleanup;
inspect the estate before continuing or retrying.

`etl` follows the native package's entity order, clear statements and result-set
schemas, with an explicit cutoff and cutoff-year Date population. Its result
metadata preflight is read-only. It verifies this run's 13 lineage keys after
loading. A failure can leave completed earlier entities and an unfinished lineage
row; there is no automatic retry or rollback across entities. Coordinate windows
and cutoffs yourself; no reset is implemented.

Destination columns follow the native dtsx input-to-external-column mapping,
with a name-match fallback where a mapping is absent. This includes Movement
`Last Modified When` → `Last Modifed When` and Purchase
`Is Order Line Finalized` → `Is Order Finalized`. Missing or duplicate mapped
destinations fail the read-only preflight; no live schema changes are made.

Successful mutating runs write `golden/.capture_state.json` beside the script;
export records its window only when the live cutoffs match that run. Otherwise
the window is unknown (`null`) and actual live per-entity cutoffs are recorded.
`native_result_sets.json` is the native dtsx's WITH RESULT SETS metadata;
`native_destination_columns.json` preserves its OLE DB destination mappings;
`ssis_branches.json` is the Step 1 catalog inventory, with deduplicated missing
procedure sets for the 205 extended packages. These are not dynamically refreshed
by `manifest`; regenerate/review them if the legacy estate changes.
