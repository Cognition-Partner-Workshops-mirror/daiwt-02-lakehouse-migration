# G08 Transaction Type dimension

`wwi_dim_transaction_type_to_gold` publishes `gold.dim_transaction_type`.
D0 restores P5's D0 golden seed, including the key-0 unknown member.
D1/D2 use P3's SCD2 renderer on S07's `(last_cutoff, new_cutoff]` changes:
close open versions at the minimum staged start, then insert every version.
Keys continue from P5's high-water mark, ordered by valid_from then business
key (valid_to and name break ties). Identical occurrences are retained.
Validity strings preserve all seven fractional digits. Integration supplies
the run's Transaction Type lineage key; P5 owns cutoff/lineage completion.

Delta table properties `g08.D1` and `g08.D2` retain the pre-drop version,
allocated HWM/boundary, count and window before any data mutation. Replay first
restores that snapshot using INSERT OVERWRITE, then repeats SCD2 with the same
allocation. This also recovers partial close/insert failures and empty stages.
Keep the pre-drop Delta history/data files available: do not VACUUM away these
versions during replay. One writer and immutable silver staging are required.
Replay a drop before proceeding to the next; to replay D1 after D2, restart D0.
D0 clears the checkpoints. No extra owned tables are created.

The visible `parity` job task uses `src/parity` against the run drop's golden
CSV and fails on differences, including an empty target. Job parameters are
`drop`, `last_cutoff`, `new_cutoff`; bundle variables supply catalog, schemas
and golden path. Silver's window is checked before incrementally loading.
`users` inherits CAN_VIEW from both bundle targets, without a duplicate grant.

The registry depends on P5 and S07. New-registry-entry classification includes
their upstream closure, so `ci/dev_verify.sh pr_<number>` runs bronze, temporal
views, P5, S07 and G08 in topological order for each of D0, D1 and D2.
The job also invokes S07 as its first task with the same window: classification
of later G08-only edits does not select upstream dependencies. On a full run
S07 consequently executes twice, safely refreshing the same window. P5 must
already have seeded Integration for an explicit G08-only invocation; explicit
selection expands dependants, not dependencies.

Local checks: `PYTHONPATH=src:ci python -m pytest -q
src/sql/units/wwi_dim_transaction_type_to_gold tests/test_registry.py tests/test_ci.py`.
