"""Exercise P9 quality on isolated, development-only customer-shaped rows."""
import argparse
import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from parity.parity_check import identifier


def exercise(spark, catalog, schema):
    if catalog != "daiwt_02_mig_dev" or not re.fullmatch(r"silver_pr_[0-9]+", schema):
        raise ValueError("Bad-row fixture requires the dev catalog and silver_pr_<n> schema")
    try:
        from governance.quality import quarantine_sql
    except ImportError as exc:
        raise RuntimeError("P9 governance.quality is required; merge PR #28 before running this task") from exc
    prefix = ".".join(identifier(p) for p in (catalog, schema))
    source, quarantine = prefix + ".`dev_bad_rows`", prefix + ".`dev_bad_rows_quarantine`"
    with (Path(__file__).parent / "fixtures/bad_rows.csv").open() as stream:
        rows = [tuple(value or None for value in row) + ("DEV_BAD",) for row in list(csv.reader(stream))[1:]]
    frame = spark.createDataFrame(rows, "wwi_customer_id STRING, customer STRING, valid_from STRING, valid_to STRING, _drop STRING")
    frame.write.mode("overwrite").saveAsTable(source)
    rules = {"customer_id_valid": "try_cast(wwi_customer_id AS BIGINT) IS NOT NULL",
             "valid_window": "try_cast(valid_from AS TIMESTAMP) < try_cast(valid_to AS TIMESTAMP)"}
    for statement in quarantine_sql(source, quarantine, rules, drop="DEV_BAD"):
        spark.sql(statement)
    count = spark.sql(f"SELECT count(*) FROM {quarantine} WHERE _drop = 'DEV_BAD'").collect()[0][0]
    if count != len(rows):
        raise RuntimeError(f"Expected {len(rows)} fixture rejects, got {count}")
    print(f"PASS: {count} DEV_BAD rows in {quarantine}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    exercise(SparkSession.builder.getOrCreate(), args.catalog, args.schema)


if __name__ == "__main__":
    main()
