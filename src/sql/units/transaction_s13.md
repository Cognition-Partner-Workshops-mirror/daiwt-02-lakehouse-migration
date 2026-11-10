# S13 Transaction

`wwi_transaction_to_silver` refreshes the serverless Lakeflow pipeline and runs
strict parity for the runner's `drop`, `last_cutoff`, and `new_cutoff` parameters.
It owns only `silver.transaction_changes` and `silver.transaction_quarantine`;
gold belongs to G14. The registry dependency is `bronze_landing_replay` (P4).
P6 is not required or referenced.

## P4 interface

The extract uses P4's all-STRING audit tables:
- `bronze.sales_customer_transactions_raw`
- `bronze.purchasing_supplier_transactions_raw`
- `bronze.sales_invoices_raw`

Their source columns are accompanied by `_drop`, `_op` (base/upsert/delete),
`_file`, and `_ingested_at`; P4 does not expose a row ordinal. These three
sources have primary keys and at most one upsert per key per drop. Latest
drop per key reconstructs the state; an upsert wins over a same-drop delete.
Deletes otherwise remove the key. P4's typed current tables are not used,
because they cannot reconstruct earlier drops after subsequent replay.

The pipeline emits each manifest window only when all three source tables have
PASS records for that drop in P4's `bronze_replay_log`. It reconstructs every
completed drop separately from raw history, retaining repeated extraction events
across drops. D0 uses the reseed cutoff 2012-12-31 and entity cutoff
2016-06-06 12:31:17, while D1/D2 use manifest cutoffs.

## Runner and verification

The runner selects downstream dependants, not upstream prerequisites. For a
clean per-drop replay, select both jobs:
`bash ci/dev_verify.sh pr_14 bronze_landing_replay,wwi_transaction_to_silver`.
This cleans selected ownership, then runs P4 followed by S13 for D0, D1, D2.
No separate bronze population or S13 bronze tasks are needed.

The job's parity task reads golden CSVs with `read_files`, subtracts the previous
full output as a multiset before projecting, and fails with `raise_error` unless
counts are 99,585 / 705 / 842, both EXCEPT ALL differences are zero, and quarantine
is empty. It returns the metrics as notebook output. The shared P0/P1 gold
contract intentionally has no S13 output registration: shared gold parity SKIPs
unmigrated gold, while this job independently enforces silver parity.

Silver columns follow the native result-set declaration, with `_drop` and
`_source_kind` appended. INT holds the bit values 0/1; datetime2 is TIMESTAMP_NTZ.
Window filtering uses original seven-digit strings before casting.

All expectations warn. Quarantine additionally copies failures other than total
arithmetic. Golden payments legitimately have zero excluding-tax and tax amounts
but a nonzero transaction amount: arithmetic warns for 27,003 / 172 / 191 rows.
These remain in silver and do not enter quarantine. Signed balances are accepted.
