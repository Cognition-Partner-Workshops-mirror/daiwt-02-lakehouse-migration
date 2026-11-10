"""Fail publication when registered silver objects are missing or quarantined."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from parity.parity_check import identifier, literal


def verify(spark, registry, catalog, schema, drop):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Only golden replay drops may be verified")
    silver = sorted({t for spec in registry.values() for t in spec["outputs"] + spec["owns"]
                     if t.startswith("silver.")})
    for table in silver:
        target = ".".join(identifier(p) for p in (catalog, schema, table.split(".")[1]))
        spark.sql(f"SELECT * FROM {target} LIMIT 0").collect()
        if table.endswith("_quarantine"):
            count = spark.sql(f"SELECT count(*) FROM {target} WHERE `_drop` = {literal(drop)}").collect()[0][0]
            if count:
                raise RuntimeError(f"{table}: {count} quarantined rows for {drop}")
    return len(silver)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--drop", required=True, choices=("D0", "D1", "D2"))
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    registry = json.loads((Path(__file__).resolve().parents[1] / "sql/units/registry.json").read_text())
    count = verify(SparkSession.builder.getOrCreate(), registry, args.catalog, args.schema, args.drop)
    print(f"PASS {args.drop}: {count} registered silver objects; quarantine empty")


if __name__ == "__main__":
    main()
