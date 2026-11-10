# Unity Catalog governance and quality

The governance job is the **last task** in the P8 migration DAG, after all
publications. Its registry entry deliberately has no business outputs or owned
tables. The DAG must explicitly depend on all publication tasks; registry order
alone is not a DAG dependency.

Every bronze, silver and gold table receives a table comment and UC tags:
`layer`, `source_entity`, `legacy_object`, `data_owner`, `managed_by`.
`source_entity` uses the 13 snake_case WWI extraction entities, or `integration`,
`date`, `report`, `unclassified`. Quarantine tables additionally receive
`quality_role=quarantine`. The default owner group is
`daiwt-02-data-engineering`; `managed_by` is `daiwt-02-migration`.
Ownership is recorded in **the data_owner tag only**. The deploying identity
retains UC ownership so CI cleanup can drop tables; no `ALTER ... OWNER TO`
is issued. Gold column comments use the parity contract's positional legacy DW
names and native destination mapping where available. Known audit columns are
commented in every layer; unknown columns are left alone.

`catalog.json` defines gold and silver metadata. Bronze legacy objects are
derived from manifest input filenames, including archive and raw variants.
Unknown tables receive generic comments and `source_entity=unclassified`.
Materialized views and streaming tables use `COMMENT ON TABLE` plus
`ALTER MATERIALIZED VIEW` / `ALTER STREAMING TABLE` for tags and column comments.
Ordinary views receive only a table comment. Lakeflow builders may also pass
`lakeflow_table_kwargs(layer, table)` to decorators: the helper supplies a
comment and `daiwt.*` table properties (properties are **not** UC tags); the final
governance job still applies the UC tags.

## Non-destructive quality

Rules map a rule name to SQL that is TRUE for a valid row; NULL fails. Use
WARN expectations and a diagnostic quarantine copy, never `expect_or_drop` or
`expect_or_fail`. Original rows must remain available for golden parity, and
quarantine must be empty for golden inputs. Do not silently filter invalid rows
from the business table. Rule expressions are trusted SQL authored by builders.

```python
from pyspark import pipelines as dp
from governance import lakeflow_table_kwargs
from governance.quality import warn_expectations, quarantine_frame

quality = {"customer_id_present": "customer_id IS NOT NULL"}

@dp.table(name="customer_changes", **lakeflow_table_kwargs("silver", "customer_changes"))
@dp.expect_all(warn_expectations(quality))
def customer_changes():
    return spark.read.table("bronze.sales_customers")

@dp.materialized_view(name="customer_changes_quarantine",
                     **lakeflow_table_kwargs("silver", "customer_changes_quarantine"))
def customer_changes_quarantine():
    return quarantine_frame(spark.read.table("customer_changes"), quality)
```

SQL builders execute `quarantine_sql(source, quarantine, quality, drop="D0")`
statements in order using `spark.sql`. Source silver tables carry a STRING
`_drop` column; quarantine keeps it and adds `_failed_rules ARRAY<STRING>` and
`_quarantined_at TIMESTAMP`. An explicit drop deletes/replaces only that drop's
diagnostic rows. With `drop=None`, all drops present in source are replaced.
The source is never mutated. Run serially per quarantine table (DELETE/INSERT
is not an atomic transaction). An interrupted rerun must be retried to completion.
Use `<silver_table>_quarantine` in the same schema, and list both tables in the
builder unit registry `owns`, for example:

```python
from governance.quality import quarantine_sql

for statement in quarantine_sql(
    "daiwt_02_mig_dev.silver_pr_123.customer_changes",
    "daiwt_02_mig_dev.silver_pr_123.customer_changes_quarantine",
    {"customer_id_present": "customer_id IS NOT NULL"}, drop="D0",
):
    spark.sql(statement)
```

```json
{"job": "silver_customer", "outputs": [], "owns": ["silver.customer_changes", "silver.customer_changes_quarantine"], "depends_on": []}
```

## Execution boundaries

`PYTHONPATH=src python -m governance.apply_governance --catalog daiwt_02_mig_dev
--schemas bronze_pr_123,silver_pr_123,gold_pr_123 --warehouse-id 6dc4b80538621b7b`
uses Statement Execution locally; omit `--warehouse-id` in a Spark task.
The apply step refuses any other catalog unless `--allow-prod` is explicit.
Production execution is reserved for Devin-CI; the production bundle does not
automatically opt in. Production grants and schemas are managed only through
approval-gated Terraform PRs in `dbx-data-platform-infra`. Dev job permissions
grant `users` CAN_VIEW; governance does not grant data access.

`PYTHONPATH=src python -m governance.smoke pr_123` renders sequential SQL for
fresh fixture tables, quality assertions, metadata assertions and table-only
cleanup. Optional `--warehouse-id` executes it. It never replaces existing
tables. Add `--keep` to retain fixtures for a subsequent deployed job check.
It never drops schemas; use an unused PR suffix. Failure leaves fixtures for
inspection. No production catalog is accepted by the smoke generator.
