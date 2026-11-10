"""Render dev-only fixture SQL: PYTHONPATH=src python -m governance.smoke pr_123.

Pass --warehouse-id to execute instead. No existing table is replaced. Execute
sequentially: assertion failure stops before cleanup, preserving the evidence.
"""

import argparse
import re

from . import DEV_CATALOG, governance_statements, literal, resolve
from .quality import quarantine_sql


def statements(suffix):
    if not re.fullmatch(r"pr_[0-9]+", suffix):
        raise ValueError("smoke requires pr_<n>")
    fixtures = [
        ("bronze", "sales_customers", ["customer_id", "_drop"]),
        ("silver", "customer_changes", ["customer_id", "_drop"]),
        ("gold", "dim_customer", ["customer_key", "customer", "_drop"]),
        ("silver", "customer_changes_quarantine", ["customer_id", "_drop", "_failed_rules", "_quarantined_at"]),
    ]
    for layer in ("bronze", "silver", "gold"):
        yield f"CREATE SCHEMA IF NOT EXISTS {DEV_CATALOG}.{layer}_{suffix}"
    bronze = f"{DEV_CATALOG}.bronze_{suffix}.sales_customers"
    silver = f"{DEV_CATALOG}.silver_{suffix}.customer_changes"
    gold = f"{DEV_CATALOG}.gold_{suffix}.dim_customer"
    quarantine = silver + "_quarantine"
    yield f"CREATE TABLE {bronze} (customer_id INT, _drop STRING) USING DELTA"
    yield f"INSERT INTO {bronze} VALUES (1, 'D0'), (2, 'D0')"
    yield f"CREATE TABLE {silver} (customer_id INT, _drop STRING) USING DELTA"
    yield f"INSERT INTO {silver} VALUES (1, 'D0'), (NULL, 'D0'), (-1, 'D0'), (2, 'D1')"
    yield f"CREATE TABLE {gold} (customer_key INT, customer STRING, _drop STRING) USING DELTA"
    yield f"INSERT INTO {gold} VALUES (1, 'Example customer', 'D0')"
    rules = {"positive_id": "customer_id > 0"}
    # Require a fresh quarantine fixture rather than reusing another run's table.
    yield f"CREATE TABLE {quarantine} USING DELTA AS SELECT customer_id, CAST(array() AS ARRAY<STRING>) AS _failed_rules, _drop, current_timestamp() AS _quarantined_at FROM {silver} WHERE false"
    for _ in range(2):
        yield from quarantine_sql(silver, quarantine, rules, "D0")
        yield f"SELECT assert_true(count(*) = 4) FROM {silver}"
        yield f"SELECT assert_true(count(*) = 2 AND count_if(_failed_rules = array('positive_id')) = 2) FROM {quarantine}"
    for layer, table, columns in fixtures:
        schema = f"{layer}_{suffix}"
        yield from governance_statements(DEV_CATALOG, schema, layer, table, columns, "MANAGED")
        where = f"table_schema = '{schema}' AND table_name = '{table}'"
        info = f"{DEV_CATALOG}.information_schema"
        metadata = resolve(layer, table)
        yield f"SELECT assert_true(count(*) = 1 AND max(comment) = {literal(metadata['comment'])}) FROM {info}.tables WHERE {where}"
        for key, value in metadata["tags"].items():
            yield f"SELECT assert_true(count(*) = 1) FROM {info}.table_tags WHERE {where} AND tag_name = {literal(key)} AND tag_value = {literal(value)}"
        yield f"SELECT assert_true(count(*) = 1 AND max(comment) = 'Golden extraction drop identifier.') FROM {info}.columns WHERE {where} AND column_name = '_drop'"
        # The convention sets column comments, not column tags.
        yield f"SELECT assert_true(count(*) = 0) FROM {info}.column_tags WHERE {where}"
    yield f"SELECT assert_true(count(*) = 1 AND max(comment) = 'Legacy DW column: Customer Key') FROM {DEV_CATALOG}.information_schema.columns WHERE table_schema = 'gold_{suffix}' AND table_name = 'dim_customer' AND column_name = 'customer_key'"
    for layer, table, _ in reversed(fixtures):
        yield f"DROP TABLE {DEV_CATALOG}.{layer}_{suffix}.{table}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suffix")
    parser.add_argument("--warehouse-id")
    args = parser.parse_args()
    sql = list(statements(args.suffix))
    if args.warehouse_id:
        from .apply_governance import warehouse_executor

        execute = warehouse_executor(args.warehouse_id)
        for statement in sql:
            execute(statement)
        print("Governance smoke: PASS")
    else:
        print(";\n\n".join(sql) + ";")


if __name__ == "__main__":
    main()
