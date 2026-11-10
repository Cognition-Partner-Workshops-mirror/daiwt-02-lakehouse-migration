"""Spark operations for S11; no SQL Server connections or gold writes."""
import json
from pathlib import Path

from s11_sale.sale import (COLUMNS, ID_DIMENSIONS, SHARED, assert_parity,
                           cutoff, drop_window, quote, target_namespace)


def read_csv(spark, root, relative, columns):
    return (spark.read.format("csv").option("header", True).option("multiLine", True)
            .option("mode", "FAILFAST").option("enforceSchema", False)
            .option("quote", '"').option("escape", '"').option("nullValue", "\\N")
            .option("emptyValue", "")
            .schema(", ".join(f"{quote(c)} STRING" for c in columns))
            .load(f"{root}/{relative}"))


def manifest_at(root):
    return json.loads(Path(root, "manifest.json").read_text())


def validate_bronze(spark, bronze, manifest, drop, last, new):
    expected = {path.split("/")[-1].removeprefix("WideWorldImporters.").removesuffix(".csv")
                for path in manifest["files"] if path.startswith(f"inputs/{drop}/")
                and not path.endswith(".deleted.csv")}
    rows = spark.sql(f"SELECT * FROM {bronze}.bronze_replay_log WHERE drop_index = "
                     f"(SELECT max(drop_index) FROM {bronze}.bronze_replay_log)").collect()
    passed = [row["source_table"] for row in rows if row["status"] == "PASS"]
    if not rows or any(row["drop"] != drop for row in rows) or len(passed) != len(expected) or set(passed) != expected:
        raise ValueError("P4 has not completed the requested bronze drop")
    windows = spark.table(f"{bronze}.golden_drops").where("table_name = 'Sale'").collect()
    selected = [row for row in windows if row["drop"] == drop]
    if len(selected) != 1 or tuple(cutoff(selected[0][key]) for key in ("last_cutoff", "new_cutoff")) != (last, new):
        raise ValueError("P4 Sale window does not match requested cutoffs")


def parity_frames(spark, root, manifest, drop, actual):
    """Compare strings, not floats or hashes; preserve duplicates with EXCEPT ALL."""
    path = f"outputs/{drop}/tables/Fact.Sale.csv"
    current = read_csv(spark, root, path, manifest["files"][path]["columns"])
    if drop == "D0":
        expected = current.limit(0)
    else:
        path = f"outputs/D{int(drop[1:]) - 1}/tables/Fact.Sale.csv"
        previous = read_csv(spark, root, path, manifest["files"][path]["columns"])
        expected = current.join(previous.select("sale_key"), "sale_key", "left_anti")

    actual.createOrReplaceTempView("sale_actual")
    expected.createOrReplaceTempView("sale_expected")
    actual_expr = [f"CAST(a.{quote(c)} AS STRING) AS {quote(c)}" for c in SHARED]
    expected_expr = [f"e.{quote(c.lower().replace(' ', '_'))} AS {quote(c)}" for c in SHARED]
    joins = []
    for index, (column, key, dimension, natural) in enumerate(ID_DIMENSIONS):
        path = f"outputs/{drop}/tables/{dimension}.csv"
        dim = read_csv(spark, root, path, manifest["files"][path]["columns"])
        view = f"sale_dim_{index}"
        dim.createOrReplaceTempView(view)
        dim_key = {"bill_to_customer_key": "customer_key", "salesperson_key": "employee_key"}.get(key, key)
        joins.append(f"LEFT JOIN {view} d{index} ON e.{key} = d{index}.{dim_key} AND e.{key} <> '0'")
        expected_expr.append(f"d{index}.{natural} AS {quote(column)}")
        actual_expr.append(f"""CASE WHEN EXISTS (
            SELECT 1 FROM {view} d WHERE d.{natural} = CAST(a.{quote(column)} AS STRING)
            AND d.{dim_key} <> '0'
            AND a.`Last Modified When` > CAST(d.valid_from AS TIMESTAMP_NTZ)
            AND a.`Last Modified When` <= CAST(d.valid_to AS TIMESTAMP_NTZ)
            ) THEN CAST(a.{quote(column)} AS STRING) END AS {quote(column)}""")
    actual_strings = spark.sql(f"SELECT {', '.join(actual_expr)} FROM sale_actual a")
    expected_strings = spark.sql(f"SELECT {', '.join(expected_expr)} FROM sale_expected e {' '.join(joins)}")
    return actual_strings, expected_strings


def check(spark, catalog, bronze_schema, silver_schema, root, drop, last="", new=""):
    manifest = manifest_at(root)
    last, new = drop_window(manifest, drop, last, new)
    bronze = target_namespace(catalog, bronze_schema, "bronze")
    silver = target_namespace(catalog, silver_schema, "silver")
    validate_bronze(spark, bronze, manifest, drop, last, new)
    actual = spark.table(f"{silver}.sale_changes")
    if actual.columns != [c for c, _ in COLUMNS]:
        raise ValueError("Sale result column order differs from native contract")
    actual_strings, expected_strings = parity_frames(spark, root, manifest, drop, actual)
    result = assert_parity(actual_strings.count(), expected_strings.count(),
                           actual_strings.exceptAll(expected_strings).count(),
                           expected_strings.exceptAll(actual_strings).count(),
                           spark.table(f"{silver}.sale_changes_quarantine").count())
    result.update(drop=drop, last_cutoff=last, new_cutoff=new)
    print(json.dumps(result, sort_keys=True), flush=True)
    if result["status"] != "PASS":
        raise AssertionError(json.dumps(result))
    return result
