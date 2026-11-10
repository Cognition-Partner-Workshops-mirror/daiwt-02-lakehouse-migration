"""Single-writer Integration helpers for G-units and the daily DAG."""

import re
from datetime import datetime

from sql.framework import fmt_datetime7, identifier, relation

ENTITIES = ("City", "Customer", "Employee", "Payment Method", "Stock Item",
            "Supplier", "Transaction Type", "Movement", "Order", "Purchase",
            "Sale", "Stock Holding", "Transaction")


def cutoff_literal(value):
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{7}", value):
        raise ValueError("cutoff must be datetime2(7) text")
    datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
    return f"'{value}'"


def allocate_keys(high_water_mark, count):
    """Reserve before deleting rows; retain returned HWM in caller run state."""
    if any(type(v) is not int or v < 0 for v in (high_water_mark, count)):
        raise ValueError("HWM and count must be nonnegative integers")
    return range(high_water_mark + 1, high_water_mark + count + 1), high_water_mark + count


def high_water_mark(spark, table, key):
    return int(spark.sql(f"SELECT coalesce(max({identifier(key)}), 0) FROM {relation(table)}").first()[0])


class Integration:
    def __init__(self, spark, catalog, schema):
        self.spark = spark
        self.lineage = relation(f"{catalog}.{schema}.integration_lineage")
        self.cutoffs = relation(f"{catalog}.{schema}.integration_etl_cutoff")

    def begin_run(self, cutoff):
        """Reserve the full ordered block before parallel entity tasks start."""
        value = cutoff_literal(cutoff)
        existing = self.spark.sql(
            f"SELECT lineage_key, table_name FROM {self.lineage} WHERE source_system_cutoff_time = {value}").collect()
        if existing:
            keys = {row[1]: int(row[0]) for row in existing}
            if set(keys) != set(ENTITIES) or len(existing) != len(ENTITIES):
                raise ValueError("Incomplete lineage block")
            return keys
        hwm = int(self.spark.sql(f"SELECT coalesce(max(lineage_key), 0) FROM {self.lineage}").first()[0])
        keys, _ = allocate_keys(hwm, len(ENTITIES))
        rows = ", ".join(f"({key}, '{entity}')" for key, entity in zip(keys, ENTITIES))
        now = fmt_datetime7("current_timestamp()", source="timestamp")
        self.spark.sql(f"INSERT INTO {self.lineage} SELECT k, {now}, entity, CAST(NULL AS STRING), 0, {value} FROM VALUES {rows} AS v(k, entity)").collect()
        return dict(zip(ENTITIES, keys))

    def start_lineage(self, table_name, cutoff):
        if table_name not in ENTITIES:
            raise ValueError("Unknown entity")
        return self.begin_run(cutoff)[table_name]

    def complete_lineage(self, key):
        if type(key) is not int or key <= 0:
            raise ValueError("lineage key must be positive")
        now = fmt_datetime7("current_timestamp()", source="timestamp")
        self.spark.sql(f"UPDATE {self.lineage} SET data_load_completed = greatest(data_load_started, {now}), was_successful = 1 WHERE lineage_key = {key} AND was_successful = 0").collect()

    def get_cutoff(self, table_name):
        if table_name not in (*ENTITIES, "Date"):
            raise ValueError("Unknown entity")
        return self.spark.sql(f"SELECT cutoff_time FROM {self.cutoffs} WHERE table_name = '{table_name}'").first()[0]

    def advance_cutoffs(self, cutoff):
        names = ", ".join(f"'{entity}'" for entity in ENTITIES)
        self.spark.sql(f"UPDATE {self.cutoffs} SET cutoff_time = {cutoff_literal(cutoff)} WHERE table_name IN ({names})").collect()
