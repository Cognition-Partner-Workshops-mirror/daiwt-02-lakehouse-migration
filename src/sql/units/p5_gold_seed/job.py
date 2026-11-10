# Databricks notebook source
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd().parents[2]))

from sql.units.p5_gold_seed.integration import ENTITIES, Integration, cutoff_literal
from sql.units.p5_gold_seed.seed import seed


def run(spark, contract, *, catalog, gold_schema, golden_path, drop, last_cutoff, new_cutoff):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Unknown drop")
    cutoff_literal(last_cutoff)
    cutoff_literal(new_cutoff)
    if new_cutoff < last_cutoff:
        raise ValueError("Cutoff window is reversed")
    if drop == "D0":
        seed(spark, contract, catalog, gold_schema, golden_path, replace=True)
        return
    control = Integration(spark, catalog, gold_schema)
    for entity in ENTITIES:
        if control.get_cutoff(entity) not in (last_cutoff, new_cutoff):
            raise ValueError("Replay requires the preceding drop")
    keys = control.begin_run(new_cutoff)
    for entity in ENTITIES:
        control.complete_lineage(keys[entity])
    control.advance_cutoffs(new_cutoff)


if __name__ == "__main__":
    contract = json.loads((Path.cwd().parents[2] / "parity/contract.json").read_text())
    run(spark, contract, **{name: dbutils.widgets.get(name) for name in
                           ("catalog", "gold_schema", "golden_path", "drop", "last_cutoff", "new_cutoff")})
