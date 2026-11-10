"""Spark operations for S11; no SQL Server connections or gold writes."""
import json
from pathlib import Path

from s11_sale.sale import (COLUMNS, ID_DIMENSIONS, SHARED, SOURCES, assert_parity,
                           drop_window, namespace, quote, source_type)


def read_csv(spark, root, relative, columns):
    return (spark.read.format("csv").option("header", True).option("multiLine", True)
            .option("mode", "FAILFAST").option("enforceSchema", False)
            .option("quote", '"').option("escape", '"').option("nullValue", "\\N")
            .option("emptyValue", "")
            .schema(", ".join(f"{quote(c)} STRING" for c in columns))
            .load(f"{root}/{relative}"))


def manifest_at(root):
    return json.loads(Path(root, "manifest.json").read_text())


def typed_projection(columns):
    converted = {c: (f"unhex({quote(c)})" if source_type(c) == "BINARY" else
                     f"try_cast({quote(c)} AS {source_type(c)})") for c in columns}
    checks = [f"({quote(c)} IS NULL OR {expr} IS NOT NULL)"
              for c, expr in converted.items()]
    return [f"{expr} AS {quote(c)}" for c, expr in converted.items()] + [
        " AND ".join(checks) + " AS _cast_valid", "_drop", "_file", "_ingested_at"]


def ingest(spark, catalog, schema, root, drop, last="", new=""):
    from pyspark.sql import functions as F

    bronze = namespace(catalog, schema)
    if catalog.endswith("_prod"):
        raise ValueError("S11 stand-in ingest is dev-only")
    manifest = manifest_at(root)
    last, new = drop_window(manifest, drop, last, new)
    context = f"{bronze}.sale_run_context"
    if drop != "D0":
        previous = spark.table(context).collect()
        predecessor = f"D{int(drop[1:]) - 1}"
        if len(previous) != 1 or not (
            (previous[0]["drop"] == predecessor and previous[0]["status"] == "COMPLETE")
            or previous[0]["drop"] == drop
        ):
            raise ValueError("Replay requires the previous drop's completed parity check")

    for source, (table, key) in SOURCES.items():
        relative = f"inputs/{drop}/WideWorldImporters.{source}.csv"
        entry = manifest["files"][relative]
        columns = entry["columns"]
        if entry["primary_key"] != [key]:
            raise ValueError(f"Unexpected identity for {source}")
        raw_table, current_table = f"{bronze}.{table}_raw", f"{bronze}.{table}"

        def landing(path, cols):
            frame = read_csv(spark, root, path, cols)
            frame = frame.select(*[F.col(c) if c in cols else F.lit(None).cast("string").alias(c)
                                   for c in columns])
            frame = (frame.withColumn("_drop", F.lit(drop)).withColumn("_file", F.lit(path))
                     .withColumn("_ingested_at", F.current_timestamp()))
            frame.write.format("delta").mode("append").saveAsTable(raw_table)
            return frame

        deleted = relative.removesuffix(".csv") + ".deleted.csv"
        if drop != "D0" and deleted in manifest["files"]:
            deletion = landing(deleted, manifest["files"][deleted]["columns"])
            deletion.selectExpr(f"try_cast({quote(key)} AS INT) AS {quote(key)}").createOrReplaceTempView("sale_deletes")
            spark.sql(f"MERGE INTO {current_table} t USING sale_deletes s ON t.{quote(key)} = s.{quote(key)} WHEN MATCHED THEN DELETE")

        upserts = landing(relative, columns).selectExpr(*typed_projection(columns))
        if drop == "D0":
            upserts.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(current_table)
        else:
            upserts.createOrReplaceTempView("sale_upserts")
            spark.sql(f"""MERGE INTO {current_table} t USING sale_upserts s
                ON t.{quote(key)} = s.{quote(key)}
                WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")

    spark.createDataFrame([(drop, last, new, "PENDING")],
                          "drop STRING, last_cutoff STRING, new_cutoff STRING, status STRING") \
        .write.format("delta").mode("overwrite").saveAsTable(context)
    print(json.dumps({"drop": drop, "last_cutoff": last, "new_cutoff": new, "status": "INGESTED"}))


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
    bronze, silver = namespace(catalog, bronze_schema), namespace(catalog, silver_schema)
    context = spark.table(f"{bronze}.sale_run_context").collect()
    if len(context) != 1 or (context[0]["drop"], context[0]["last_cutoff"], context[0]["new_cutoff"]) != (drop, last, new):
        raise ValueError("Pipeline context does not match parity window")
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
    if catalog.endswith("_prod"):
        raise ValueError("S11 stand-in parity is dev-only")
    spark.sql(f"UPDATE {bronze}.sale_run_context SET status = 'COMPLETE'")
    return result
