"""Check registered objects and mapped golden counts; report quarantine copies."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from parity.parity_check import identifier, literal


def golden_counts(manifest, bronze_spec, contract, drop):
    counts = {entry["table"]: entry["row_count"] for entry in contract["entries"]
              if entry["drop"] == drop and entry["table"].startswith(("silver.", "bronze."))}
    for table in bronze_spec["tables"]:
        meta = manifest["files"].get(f"inputs/{drop}/{table['file']}", {})
        if "full_row_count" in meta:
            counts[f"bronze.{table['table']}"] = meta["full_row_count"]
    return counts


def verify(spark, registry, catalog, schema, drop, *, expected_counts=None, bronze_schema=None):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("Only golden replay drops may be verified")
    registered = {t for spec in registry.values() for t in spec["outputs"] + spec["owns"]}
    silver = sorted(t for t in registered if t.startswith("silver."))
    for table in silver:
        target = ".".join(identifier(p) for p in (catalog, schema, table.split(".")[1]))
        frame = spark.sql(f"SELECT * FROM {target} LIMIT 0")
        frame.collect()
        if table.endswith("_quarantine"):
            drop_column = next((column for column in ("_drop", "drop") if column in frame.columns), None)
            if drop_column is None:
                raise RuntimeError(f"{table}: quarantine table requires an _drop or drop column")
            count = spark.sql(f"SELECT count(*) FROM {target} WHERE {identifier(drop_column)} = {literal(drop)}").collect()[0][0]
            print(f"QUARANTINE {drop}: {table}: {count} rows")
    for table, expected in sorted((expected_counts or {}).items()):
        if table not in registered or table.endswith("_quarantine"):
            continue
        layer, name = table.split(".")
        target_schema = schema if layer == "silver" else bronze_schema
        if target_schema is None:
            raise ValueError(f"{table}: bronze schema is required for golden counts")
        target = ".".join(identifier(p) for p in (catalog, target_schema, name))
        actual = spark.sql(f"SELECT count(*) FROM {target}").collect()[0][0]
        if actual != expected:
            raise RuntimeError(f"{table}: golden row-count mismatch for {drop}: expected {expected}, got {actual}")
        print(f"COUNT {drop}: {table}: {actual} rows matched golden")
    return len(silver)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--drop", required=True, choices=("D0", "D1", "D2"))
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    root = Path(__file__).resolve().parents[2]
    registry = json.loads((root / "src/sql/units/registry.json").read_text())
    counts = golden_counts(json.loads((root / "golden/manifest.json").read_text()),
                           json.loads((root / "src/bronze/tables.json").read_text()),
                           json.loads((root / "src/parity/contract.json").read_text()), args.drop)
    count = verify(SparkSession.builder.getOrCreate(), registry, args.catalog, args.schema, args.drop,
                   expected_counts=counts, bronze_schema=args.bronze_schema)
    print(f"PASS {args.drop}: {count} registered silver objects; quarantine counts logged")


if __name__ == "__main__":
    main()
