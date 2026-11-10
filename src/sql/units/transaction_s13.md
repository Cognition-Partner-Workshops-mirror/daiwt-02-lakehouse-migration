# S13 Transaction

`wwi_transaction_to_silver` replays the three golden drops, refreshes the
serverless Lakeflow pipeline, then runs strict per-drop multiset parity. It is
target-catalog parameterized. The bronze stand-in owns the P4 table names listed in the registry;
remove its tasks/ownership when replacing it with Platform P4.

Silver columns follow the native result-set declaration, with `_drop` and
`_source_kind` appended. INT holds the SQL Server bit values 0/1; datetime2 is
TIMESTAMP_NTZ in silver. Window filtering uses the original seven-digit strings
before casting, so boundary precision is retained. Each drop is extracted from
its own reconstructed bronze state, never from the final current snapshot.

All expectations warn. Quarantine additionally copies failures other than total
arithmetic. Golden payment transactions legitimately have zero excluding-tax and
tax amounts but a nonzero transaction amount: the strict arithmetic expectation
warns for 27,003 / 172 / 191 rows in D0 / D1 / D2. These remain in silver and do
not enter quarantine. Signed balances are accepted.

The job's parity task reads golden output CSVs with `read_files`, subtracts the
previous full output as a multiset before projecting, compares `EXCEPT ALL` in
both directions and counts, and fails with `raise_error` on any difference or
quarantine row. Offline tests compare all 99,585 / 705 / 842 extraction rows and
exercise invoice customer/bill-to roles, windows, empty input and deletion replay.

## Existing shared harness limitations

- `ci/run_units.py` accepts the job entry unchanged; `parity_check.py` leaves gold
  Transaction unregistered because `outputs` is empty. The job owns silver parity.
- `ci/cleanup.py` production table cleanup currently reads only `outputs`, not
  `owns`. P7's minimal fix is to union `spec.get("owns", [])` with `spec["outputs"]`
  in its table enumeration. Dev schema cleanup already covers these tables.
- `tests/test_ci.py::test_zero_units_and_bundle_targets` still asserts the entire
  registry equals `{}`. The minimal platform fix is to replace only that obsolete
  assertion with registry-entry validation (required job/outputs/owns/depends_on
  fields); preserve the remaining bundle-target assertions.
