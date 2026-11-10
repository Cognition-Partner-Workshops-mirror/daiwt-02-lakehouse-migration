# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from parity.parity_check import literal
from sql.framework import fmt_binary, fmt_datetime7, identifier, relation, scd2
from sql.units.p5_gold_seed.integration import Integration, allocate_keys, cutoff_literal, high_water_mark
from sql.units.p5_gold_seed.seed import column_type, seed

CHECKPOINT = "wwi.stock_item.checkpoint"


def save(spark, target, state):
    spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ({literal(CHECKPOINT)} = {literal(json.dumps(state))})").collect()


def run(spark, contract, *, catalog, gold_schema, silver_schema, golden_path,
        drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    last, new = cutoff_literal(last_cutoff), cutoff_literal(new_cutoff)
    if last_cutoff > new_cutoff:
        raise ValueError("Reversed window")
    table = f"{catalog}.{gold_schema}.dim_stock_item"
    target = relation(table)
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path,
             tables=["gold.dim_stock_item"], replace=True)
        spark.sql(f"ALTER TABLE {target} UNSET TBLPROPERTIES IF EXISTS ({literal(CHECKPOINT)})").collect()
        return

    silver = relation(f"{catalog}.{silver_schema}")
    window = spark.sql(f"SELECT drop, last_cutoff, new_cutoff FROM {silver}.stock_item_window").collect()
    if [tuple(row) for row in window] != [(drop, last_cutoff, new_cutoff)]:
        raise ValueError("run Stock Item silver for this window first")
    properties = dict(tuple(row) for row in spark.sql(f"SHOW TBLPROPERTIES {target}").collect())
    state = json.loads(properties.get(CHECKPOINT, "{}"))
    if drop == "D1" and "D2" in state:
        raise ValueError("Restore D0 before replaying D1 after D2")
    if drop == "D2" and not state.get("D1", {}).get("complete"):
        raise ValueError("D2 requires D1 complete")
    if drop == "D2" and state["D1"]["window"][1] != last_cutoff:
        raise ValueError("D2 must follow the D1 window")
    checkpoint = state.get(drop)
    if checkpoint and checkpoint["window"] != [last_cutoff, new_cutoff]:
        raise ValueError("Checkpoint window mismatch; restore D0 first")

    lineage = Integration(spark, catalog, gold_schema).start_lineage("Stock Item", new_cutoff)
    entry = next(e for e in contract["entries"] if e["table"] == "gold.dim_stock_item" and e["drop"] == "D0")
    columns = [c for c in entry["columns"] if c != "stock_item_key"]
    projection = []
    for column in columns:
        source = identifier(column)
        if column == "lineage_key":
            expression = f"CAST({lineage} AS BIGINT)"
        elif column in ("valid_from", "valid_to"):
            expression = fmt_datetime7(source)
        elif column == "photo":
            expression = fmt_binary(source)
        else:
            expression = f"CAST({source} AS {column_type(column)})"
        projection.append(f"{expression} AS {source}")
    stage = "stock_item_gold_stage"
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {stage} AS SELECT {', '.join(projection)} "
              f"FROM {silver}.stock_item_changes WHERE valid_from > {last} AND valid_from <= {new}").collect()
    count = int(spark.sql(f"SELECT count(*) FROM {stage}").first()[0])
    if checkpoint:
        if checkpoint["count"] != count:
            raise ValueError("Checkpoint stage count mismatch")
        spark.sql(f"RESTORE TABLE {target} TO VERSION AS OF {checkpoint['version']}").collect()
        checkpoint["complete"] = False
    else:
        hwm = high_water_mark(spark, table, "stock_item_key")
        hwm = max(hwm, state.get("D1", {}).get("reserved_hwm", 0))
        version = int(spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()[0])
        _, reserved = allocate_keys(hwm, count)
        checkpoint = dict(version=version, hwm=hwm, reserved_hwm=reserved,
                          count=count, window=[last_cutoff, new_cutoff], complete=False)
        state[drop] = checkpoint
    save(spark, target, state)
    order = ["valid_from", "wwi_stock_item_id"]
    order += [c for c in columns if c not in order]
    for statement in scd2(table, stage, key="stock_item_key",
                          business_key="wwi_stock_item_id", columns=columns,
                          staging_order=order, key_high_water_mark=checkpoint["hwm"]):
        spark.sql(statement).collect()
    checkpoint["complete"] = True
    save(spark, target, state)


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "silver_schema", "golden_path",
                            "drop", "last_cutoff", "new_cutoff")})
