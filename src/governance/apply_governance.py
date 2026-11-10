"""Apply metadata last in the migration DAG, via Spark or Statement Execution."""

import argparse
from pathlib import Path
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from governance import governance_statements, guard_catalog, identifier, literal


def warehouse_executor(warehouse_id):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    client = WorkspaceClient()

    def execute(statement):
        result = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
        while result.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            result = client.statement_execution.get_statement(result.statement_id)
        if result.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(f"Governance SQL failed: {result.status.error}")
        if not result.result:
            return []
        names = [c.name for c in result.manifest.schema.columns]
        rows = list(result.result.data_array or [])
        chunk = result.result
        while chunk.next_chunk_index is not None:
            chunk = client.statement_execution.get_statement_result_chunk_n(
                result.statement_id, chunk.next_chunk_index)
            rows.extend(chunk.data_array or [])
        return [dict(zip(names, row)) for row in rows]

    return execute


def apply_governance(catalog, schemas, execute, allow_prod=False):
    guard_catalog(catalog, allow_prod)
    governed = skipped = 0
    info = f"{identifier(catalog)}.information_schema"
    for schema in schemas:
        layer = schema.split("_", 1)[0]
        if layer not in {"bronze", "silver", "gold"}:
            raise ValueError(f"schema must start with a medallion layer: {schema}")
    for schema in schemas:
        layer = schema.split("_", 1)[0]
        tables = execute(f"SELECT table_name, table_type FROM {info}.tables WHERE table_schema = {literal(schema)} ORDER BY table_name")
        columns = execute(f"SELECT table_name, column_name FROM {info}.columns WHERE table_schema = {literal(schema)} ORDER BY ordinal_position")
        for table in tables:
            names = [c["column_name"] for c in columns if c["table_name"] == table["table_name"]]
            statements = governance_statements(catalog, schema, layer, table["table_name"], names, table["table_type"])
            if not statements:
                skipped += 1
                continue
            for statement in statements:
                execute(statement)
            governed += 1
    print(f"Governance: {governed} tables governed / {skipped} skipped")
    return governed, skipped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schemas", required=True)
    parser.add_argument("--allow-prod", action="store_true")
    parser.add_argument("--warehouse-id")
    args = parser.parse_args()
    guard_catalog(args.catalog, args.allow_prod)
    if args.warehouse_id:
        execute = warehouse_executor(args.warehouse_id)
    else:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        def execute(statement):
            return [row.asDict() for row in spark.sql(statement).collect()]

    apply_governance(args.catalog, args.schemas.split(","), execute, args.allow_prod)


if __name__ == "__main__":
    main()
