# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from parity.parity_check import literal
from sql.framework import fmt_datetime7, relation, scd2
from sql.units.p5_gold_seed.integration import Integration, allocate_keys, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import seed


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff_literal(new_cutoff)
    if new_cutoff < last_cutoff:
        raise ValueError("Cutoff window is reversed")
    target = f"{catalog}.{gold_schema}.dim_transaction_type"
    table = relation(target)
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.dim_transaction_type"], replace=True)
        spark.sql(f"ALTER TABLE {table} UNSET TBLPROPERTIES IF EXISTS ('g08.D1', 'g08.D2')").collect()
        return

    window = spark.sql(f"SELECT drop, last_cutoff, new_cutoff FROM {relation(f'{catalog}.{silver_schema}.s07_run_window')}").collect()
    if [tuple(row) for row in window] != [(drop, last_cutoff, new_cutoff)]:
        raise ValueError("Run Transaction Type silver for this window first")
    properties = dict(spark.sql(f"SHOW TBLPROPERTIES {table}").collect())
    if drop == "D1" and "g08.D2" in properties:
        raise ValueError("Replay requires the preceding drop; restore D0 first")
    if drop == "D2" and not json.loads(properties.get("g08.D1", "{}")).get("complete", False):
        raise ValueError("Replay requires the preceding drop to be complete")
    lineage = Integration(spark, catalog, gold_schema).start_lineage("Transaction Type", new_cutoff)
    stage = "g08_transaction_type_stage"
    source = relation(f"{catalog}.{silver_schema}.transaction_type_changes")
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW {stage} AS
SELECT wwi_transaction_type_id, transaction_type,
       {fmt_datetime7('valid_from')} AS valid_from,
       {fmt_datetime7('valid_to')} AS valid_to, {lineage} AS lineage_key
FROM {source}
WHERE valid_from > {cutoff_literal(last_cutoff)} AND valid_from <= {cutoff_literal(new_cutoff)}""").collect()
    count = int(spark.sql(f"SELECT count(*) FROM {stage}").first()[0])
    property_name = f"g08.{drop}"
    if property_name in properties:
        checkpoint = json.loads(properties[property_name])
        if (checkpoint["last_cutoff"], checkpoint["new_cutoff"], checkpoint["count"]) != (last_cutoff, new_cutoff, count):
            raise ValueError("Replay requires the original window and immutable stage")
        checkpoint["complete"] = False
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({literal(property_name)} = {literal(json.dumps(checkpoint))})").collect()
        spark.sql(f"INSERT OVERWRITE TABLE {table} SELECT * FROM {table} VERSION AS OF {int(checkpoint['version'])}").collect()
    else:
        if drop == "D2" and json.loads(properties["g08.D1"])["new_cutoff"] != last_cutoff:
            raise ValueError("Replay requires the preceding drop")
        hwm = high_water_mark(spark, target, "transaction_type_key")
        _, boundary = allocate_keys(hwm, count)
        version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
        checkpoint = dict(version=version, high_water_mark=hwm, boundary=boundary,
                          count=count, last_cutoff=last_cutoff, new_cutoff=new_cutoff, complete=False)
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({literal(property_name)} = {literal(json.dumps(checkpoint))})").collect()
    for statement in scd2(target, stage, key="transaction_type_key",
                          business_key="wwi_transaction_type_id",
                          columns=["wwi_transaction_type_id", "transaction_type", "valid_from", "valid_to", "lineage_key"],
                          staging_order=["valid_from", "wwi_transaction_type_id", "valid_to", "transaction_type"],
                          key_high_water_mark=checkpoint["high_water_mark"]):
        spark.sql(statement).collect()
    checkpoint["complete"] = True
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ({literal(property_name)} = {literal(json.dumps(checkpoint))})").collect()


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path", "drop", "last_cutoff", "new_cutoff")})
