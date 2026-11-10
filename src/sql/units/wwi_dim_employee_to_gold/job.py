# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.framework import fmt_datetime7, relation, scd2
from sql.units.p5_gold_seed.integration import cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import seed

COLUMNS = ("wwi_employee_id", "employee", "preferred_name", "is_salesperson", "photo",
           "valid_from", "valid_to", "lineage_key")


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff = cutoff_literal(new_cutoff)
    if new_cutoff < last_cutoff:
        raise ValueError("Cutoff window is reversed")
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.dim_employee"], replace=True)
        return
    target = f"{catalog}.{gold_schema}.dim_employee"
    lineage = relation(f"{catalog}.{gold_schema}.integration_lineage")
    rows = spark.sql(
        f"SELECT lineage_key FROM {lineage} WHERE table_name = 'Employee' "
        f"AND source_system_cutoff_time = {cutoff}").collect()
    if len(rows) != 1:
        raise ValueError("Expected exactly one Employee lineage row")
    lineage_key = int(rows[0][0])
    if spark.sql(
            f"SELECT 1 FROM {relation(target)} WHERE lineage_key = {lineage_key} LIMIT 1").collect():
        return
    staging = "dim_employee_staging"
    source = relation(f"{catalog}.{silver_schema}.employee_changes")
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW {relation(staging)} AS SELECT "
        f"CAST(wwi_employee_id AS BIGINT) AS wwi_employee_id, "
        f"CAST(employee AS STRING) AS employee, "
        f"CAST(preferred_name AS STRING) AS preferred_name, "
        f"CAST(is_salesperson AS BIGINT) AS is_salesperson, "
        f"upper(CAST(photo AS STRING)) AS photo, "
        f"{fmt_datetime7('valid_from')} AS valid_from, "
        f"{fmt_datetime7('valid_to')} AS valid_to, "
        f"CAST({lineage_key} AS BIGINT) AS lineage_key FROM {source}").collect()
    hwm = high_water_mark(spark, target, "employee_key")
    for statement in scd2(
            target, staging, key="employee_key", business_key="wwi_employee_id",
            columns=COLUMNS,
            staging_order=("valid_from", "wwi_employee_id", "employee", "preferred_name",
                           "is_salesperson", "photo"),
            key_high_water_mark=hwm):
        spark.sql(statement).collect()


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
