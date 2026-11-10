# S11 — Sale silver

`wwi_sale_to_silver` is the registry job. It defaults to a serial D0 → D1 → D2
replay, with parity after **each** drop. To run one drop after its predecessor:

```sh
databricks bundle run -t dev --var schema_suffix=pr_<n> wwi_sale_to_silver --params drop=D1
```

Replay uses explicit `d0 -> d1 -> d2` dependencies, not loop scheduling.
The internal drop job uses `sale_*` parameters so parent job parameter pushdown
cannot replace a child drop/window with the parent's `drop=all` selector.

Optional `last_cutoff` / `new_cutoff` parameters must equal the manifest's **Sale**
entry. Empty parameters resolve from the manifest (not global window dates).
D0 uses `2016-06-06 12:31:17.0000000` for both cutoffs and must produce zero
staged rows. D0 is a restored gold seed, so S11 does not attempt to regenerate it.
It resets only these five typed bronze tables; raw landing remains append-only.
Raw re-ingestion records another ingest attempt, never rewrites previous evidence.

## Bronze stand-in and temporal semantics

This unit owns P4-compatible `sales_invoices`, `sales_invoice_lines`,
`warehouse_stock_items`, `warehouse_package_types`, `sales_customers` typed tables,
their `_raw` landing tables, and `sale_run_context`. All are in the bundle's bronze
schema. Native source columns retain their names; raw source values are STRING,
with `_drop`, `_file`, `_ingested_at`. The typed counterparts cast all scalar
columns to native-compatible Spark types, retain geography as exported WKT, and
decode binary hex. Timestamps are naive `TIMESTAMP_NTZ` (Spark microsecond
precision). CSV uses multiline RFC-style doubled quotes, `\N` NULL, and preserves
empty strings. Incrementals apply manifest-listed PK deletions before MERGE
upserts; header-only files have zero rows and therefore make no changes.

`GetSaleUpdates` has **no** `FOR SYSTEM_TIME AS OF` joins: stock item, package,
customer and bill-to customer are **current state** after the drop is applied.
P6's base + archive `[ValidFrom, ValidTo)` applies to temporal dimension extracts,
not Sale; loading archives here would incorrectly duplicate Sale rows. No archive
tables or P6 views are needed. The window is `(last_cutoff, new_cutoff]` on the
legacy CASE expression over line/header modification times. Amounts are copied
at declared decimal scale; excluding tax is `ExtendedPrice - TaxAmount`.

`sale_changes` has exactly the native 20-column positional contract, including
`WWI Salesperson ID`. The query orders by InvoiceID / InvoiceLineID; consumers
must still explicitly order when reading a Delta table (physical row order is
not a table guarantee). The helper line identity is not published.
Warning-only expectations retain bad candidates; failures are also copied to
`sale_changes_quarantine`, including raw-to-typed cast/PK failures from source
tables. Missing FKs use left joins for diagnostics rather than silently losing
rows; golden input has no such failures and matches the native inner joins.

## Strict parity

D1/D2 expected rows are the current golden `Fact.Sale` rows whose `sale_key` does
not occur in the previous drop's golden fact. This includes replacement invoice
rows, not merely invoice IDs newly observed in the source. Compare all 14 shared
native fact fields (invoice/delivery date, invoice ID, description, package,
quantity, unit price, tax rate, excluding tax, tax, profit, including tax, dry and
chiller quantities), **plus all five resolvable natural IDs**. Expected IDs come
from the drop's golden dimension surrogate keys. Actual IDs are checkable only
when the golden dimension contains that ID at `Last Modified When` using the
legacy DW lookup interval `(valid_from, valid_to]`; unknown key-0 members project
to NULL. This avoids pretending to recover natural IDs from an unknown member.
No gold tables are read or written: golden dimension CSVs are read-only oracles.

Actual values are cast to golden CSV strings (date ISO, fixed-scale decimal,
integer strings, SQL NULL). Both `EXCEPT ALL` directions must be empty, row counts
must match, and quarantine must be empty. D0 asserts empty stage and quarantine
with equal manifest cutoffs. The parity task prints/returns a per-drop JSON object
with counts, both mismatch counts, cutoffs, and PASS/FAIL. Exceptions fail the job;
only PASS marks the bronze context complete. Incremental ingest requires a
completed predecessor (or retry of the same drop). Jobs serialize access to the
shared pipeline/context; do not start independent runs of the underlying pipeline
while replay is active.

## Local checks and platform handoff

```sh
python -m pip install pyspark==4.0.1  # optional local Spark integration checks
python -m pytest -q -s tests/test_s11_sale.py
python -m pytest -q
databricks bundle validate --strict -t dev --var schema_suffix=pr_0
```

Spark tests execute the production query and parity projection against reconstructed
golden D0/D1/D2 sources, plus boundary, decimal, missing-FK and malformed-cast
fixtures. Without local Spark the Spark integration tests explicitly skip. The
Databricks-only Delta MERGE and declarative decorators still need a deployed PR
run. No deployment is performed by local verification.

`depends_on: []` is intentional: the P4 stand-in and current-state joins are
self-contained, and platform registry entries do not exist yet. When P4 lands,
replace only bronze ingest/ownership and add its registered dependency. P7 cleanup
currently ignores `owns` (platform-owned follow-up); dev bundle destruction still
removes schemas, while production cleanup must not be used to infer S11 ownership.
The runtime accepts only matching dev suffixed schemas or prod bronze/silver
schemas; identity and Unity Catalog grants enforce production write access.
S11 registers `outputs: []` so
the existing global parity harness continues to SKIP G12's `gold.fact_sale`.
