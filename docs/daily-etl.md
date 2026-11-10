# Daily Lakeflow replay (P8)

`python ci/daily_etl_dag.py` regenerates the committed job from the validated
registry, the planned-unit list, and manifest cutoffs. Run all D0 tasks, publish
through D0 parity, then D1 and D2. Unit edges are exactly their declared
dependencies; silver and parity gates add publication/order constraints.
Per-unit jobs and CI remain unchanged. This supersedes the unit-major proposal
in Wave 0 §5/§6: P5 needs the preceding drop's completed gold state.

Planned S02/S03/S11/S13 and G01/G02/G04–G16 use no-compute conditions until their
registry entries land. P3 is a shared library, not a registry task; P5 is its
runtime seed prerequisite. Governance is also a placeholder until P9 (#28)
lands. Registry definitions always replace plans on regeneration. Placeholders
are wiring, not evidence of completed migration: parity still skips outputs
without registered producers. Do not enable the schedule until units are ready.

The final governance task uses NONE_FAILED after all three publication gates.
Development additionally requires the isolated bad-row exercise. It writes
only `silver_pr_<n>.dev_bad_rows` and `dev_bad_rows_quarantine`, with `_drop =
'DEV_BAD'`. Customer-shaped malformed keys/validity windows exercise P9's
`governance.quality.quarantine_sql` without modifying any business inputs or
gold. Until P9 merges the task fails clearly; no quality implementation is
vendored. The silver gate only checks registered objects and golden drop
labels D0/D1/D2, so these intentional rejects cannot affect golden parity.
Fixture tables are not registered business-unit ownership; remove them as part
of dev schema lifecycle cleanup.

## SQL Agent disposition (Wave 0 §4)

* Job 10: paused 01:30 UTC schedule, single concurrent run, queue and 7200s timeout.
* Job 15: date dimension is the registered `wwi_dim_date_to_gold` task each drop.
* Job 19: per-drop `verify_silver` requires all registered silver objects and
  zero quarantine rows for that drop. Missing objects/columns fail closed.
* Job 21: RUN_DURATION_SECONDS > 3600 health rule.
* Jobs 11–14, 16–18, 20: out of golden scope; not migrated as jobs (platform,
  maintenance, backup, expanded estate/C360 concerns per §4).
* Agent categories/operators/proxies/install scripts (00–02, 90): platform
  concerns, dropped. No email notifications: the service principal has no inbox.

Python gates use serverless environment version 2. Parity keeps its CLI and
nonzero failure exit, uses default WorkspaceClient authentication when no
profile is supplied, queries the configured SQL warehouse, and writes task-local
JSON under `/tmp`. No deployment or execution is part of this change's checks.
