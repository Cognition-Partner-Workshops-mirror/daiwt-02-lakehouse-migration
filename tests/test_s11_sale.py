import json
import re
import sys
from types import SimpleNamespace
from pathlib import Path

import pytest
import yaml

from parity.parity_check import compare
from s11_sale.sale import (COLUMNS, SOURCES, assert_parity, cutoff, drop_window,
                           namespace, sale_query, source_type, target_namespace)
from s11_sale.runtime import typed_projection

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = json.loads((ROOT / "golden/manifest.json").read_text())


def test_native_positional_contract():
    native = json.loads((ROOT / "golden/native_result_sets.json").read_text())["Sale"]
    assert re.findall(r"\[([^]]+)\]", native) == [c for c, _ in COLUMNS]
    assert COLUMNS[18] == ("WWI Salesperson ID", "INT")


@pytest.mark.parametrize("drop,last,new", [
    ("D0", "2016-06-06 12:31:17.0000000", "2016-06-06 12:31:17.0000000"),
    ("D1", "2016-06-06 12:31:17.0000000", "2016-06-14 00:00:00.0000000"),
    ("D2", "2016-06-14 00:00:00.0000000", "2016-06-21 00:00:00.0000000"),
])
def test_manifest_sale_windows(drop, last, new):
    assert drop_window(MANIFEST, drop) == (last, new)
    assert drop_window(MANIFEST, drop, last, new) == (last, new)


def test_window_mismatch_and_bad_drop():
    with pytest.raises(ValueError, match="Cutoffs differ"):
        drop_window(MANIFEST, "D1", new="2016-06-15 00:00:00")
    with pytest.raises(StopIteration):
        drop_window(MANIFEST, "D3")
    with pytest.raises(ValueError):
        cutoff("bad")
    assert cutoff("2016-06-14 01:02:03.1234567").endswith(".1234567")


def test_source_types_and_safe_names():
    assert source_type("UnitPrice") == "DECIMAL(18,2)"
    assert source_type("TaxRate") == "DECIMAL(18,3)"
    assert source_type("InvoiceID") == "INT"
    assert source_type("InvoiceDate") == "DATE"
    assert source_type("LastEditedWhen") == "TIMESTAMP_NTZ"
    assert source_type("Photo") == "BINARY"
    assert source_type("Description") == "STRING"
    assert namespace("catalog", "bronze_pr_0") == "`catalog`.`bronze_pr_0`"
    with pytest.raises(ValueError):
        namespace("catalog;DROP TABLE x", "bronze")
    assert "unhex(`Photo`) AS `Photo`" in typed_projection(["Photo"])


@pytest.mark.parametrize("catalog,schema,layer", [
    ("daiwt_02_mig_dev", "bronze_pr_20", "bronze"),
    ("daiwt_02_mig_prod", "bronze", "bronze"),
    ("daiwt_02_mig_prod", "silver", "silver"),
])
def test_target_namespace_allows_bundle_targets(catalog, schema, layer):
    assert target_namespace(catalog, schema, layer) == namespace(catalog, schema)


@pytest.mark.parametrize("catalog,schema,layer", [
    ("daiwt_02_mig_prod", "bronze_pr_20", "bronze"),
    ("daiwt_02_mig_dev", "bronze", "bronze"),
    ("daiwt_02_mig_prod", "silver", "bronze"),
    ("other", "bronze_pr_20", "bronze"),
])
def test_target_namespace_rejects_mismatches(catalog, schema, layer):
    with pytest.raises(ValueError, match="Mismatched"):
        target_namespace(catalog, schema, layer)


@pytest.mark.parametrize("counts,status", [
    ((0, 0, 0, 0, 0), "PASS"), ((2, 2, 0, 0, 0), "PASS"),
    ((1, 2, 0, 1, 0), "FAIL"), ((2, 2, 1, 1, 0), "FAIL"),
    ((2, 2, 0, 0, 1), "FAIL"),
])
def test_parity_is_strict(counts, status):
    assert assert_parity(*counts)["status"] == status


def test_registry_and_replay_bundle():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    spec = registry["wwi_sale_to_silver"]
    assert spec["outputs"] == []
    assert all(x.startswith(("bronze.", "silver.")) for x in spec["owns"])
    assert all(f"bronze.{table}" in spec["owns"] for table, _ in SOURCES.values())
    assert compare({"tables": [{"table": "gold.fact_sale"}]}, registry, None, None)["results"][0]["status"] == "SKIP"
    resources = yaml.safe_load((ROOT / "src/s11_sale/resources.yml").read_text())["resources"]
    assert spec["job"] in resources["jobs"]
    assert resources["pipelines"]["wwi_sale_silver"]["serverless"] is True
    assert [t["task_key"] for t in resources["jobs"]["wwi_sale_drop"]["tasks"]] == ["bronze", "silver", "parity"]
    parent = resources["jobs"][spec["job"]]
    tasks = {task["task_key"]: task for task in parent["tasks"]}
    assert tasks["d0"]["depends_on"] == [{"task_key": "replay_all", "outcome": "true"}]
    assert tasks["d1"]["depends_on"] == [{"task_key": "d0"}]
    assert tasks["d2"]["depends_on"] == [{"task_key": "d1"}]
    assert tasks["single_drop"]["depends_on"] == [{"task_key": "replay_all", "outcome": "false"}]
    child_params = {p["name"] for p in resources["jobs"]["wwi_sale_drop"]["parameters"]}
    assert child_params.isdisjoint(p["name"] for p in parent["parameters"])
    assert set(tasks["d0"]["run_job_task"]["job_parameters"]) == child_params


def test_pipeline_registration_without_file_global(monkeypatch):
    def decorator(*args, **kwargs):
        return lambda function: function

    dp = SimpleNamespace(temporary_view=decorator, materialized_view=decorator, expect=decorator)
    monkeypatch.setitem(sys.modules, "pyspark", SimpleNamespace(pipelines=dp))
    monkeypatch.setitem(sys.modules, "pyspark.sql", SimpleNamespace(functions=SimpleNamespace()))
    settings = {"sale.source_path": str(ROOT / "src"), "sale.catalog": "daiwt_02_mig_dev",
                "sale.bronze_schema": "bronze_pr_20"}
    globals_ = {"spark": SimpleNamespace(conf=SimpleNamespace(get=settings.__getitem__))}
    path = ROOT / "src/s11_sale/pipeline.py"
    exec(compile(path.read_text(), str(path), "exec"), globals_)
    assert callable(globals_["sale_changes"])
    assert callable(globals_["sale_changes_quarantine"])
    assert globals_["properties"]["delta.feature.timestampNtz"] == "supported"


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pyspark = pytest.importorskip("pyspark", reason="optional local Spark integration tests: pip install pyspark==4.0.1")
    session = (pyspark.sql.SparkSession.builder.master("local[2]").appName("s11-sale-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("s11-warehouse")))
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def small_sources(spark):
    sources = {
        "sales_invoices": (
            [(1, 10, 11, 3, "2016-06-13", "2016-06-14 07:05:00", "2016-06-13 00:00:00", True)],
            "InvoiceID INT, CustomerID INT, BillToCustomerID INT, SalespersonPersonID INT, InvoiceDate STRING, ConfirmedDeliveryTime STRING, LastEditedWhen STRING, _cast_valid BOOLEAN"),
        "sales_invoice_lines": (
            [(2, 1, 20, 7, "test", 120, "18.00", "15.000", "324.00", "1320.00", "2484.00", "2016-06-13 01:00:00", True)],
            "InvoiceLineID INT, InvoiceID INT, StockItemID INT, PackageTypeID INT, Description STRING, Quantity INT, UnitPrice STRING, TaxRate STRING, TaxAmount STRING, LineProfit STRING, ExtendedPrice STRING, LastEditedWhen STRING, _cast_valid BOOLEAN"),
        "warehouse_stock_items": ([(20, 0, True)], "StockItemID INT, IsChillerStock INT, _cast_valid BOOLEAN"),
        "warehouse_package_types": ([(7, "Each", True)], "PackageTypeID INT, PackageTypeName STRING, _cast_valid BOOLEAN"),
        "sales_customers": ([(10, 99, True), (11, 98, True)], "CustomerID INT, DeliveryCityID INT, _cast_valid BOOLEAN"),
    }
    for table, (rows, schema) in sources.items():
        frame = spark.createDataFrame(rows, schema)
        if table == "sales_invoice_lines":
            frame = frame.selectExpr(*[f"CAST(`{c}` AS DECIMAL(18,{3 if c == 'TaxRate' else 2})) AS `{c}`"
                                       if c in {"UnitPrice", "TaxRate", "TaxAmount", "LineProfit", "ExtendedPrice"}
                                       else f"`{c}`" for c in frame.columns])
        frame.createOrReplaceTempView(table)


def context(spark, last, new):
    spark.createDataFrame([(last, new)], "last_cutoff STRING, new_cutoff STRING").createOrReplaceTempView("sale_run_context")


def local_sale_query():
    return sale_query("spark_catalog.default").replace("spark_catalog.default.", "")


def test_spark_transform_normal_empty_boundaries_and_fk_failure(spark):
    small_sources(spark)
    context(spark, "2016-06-13 00:00:00", "2016-06-13 01:00:00")
    row = spark.sql(local_sale_query()).first()
    assert str(row["Total Excluding Tax"]) == "2160.00"
    assert str(row["Tax Rate"]) == "15.000"
    assert row["Total Dry Items"] == 120 and row["Total Chiller Items"] == 0
    assert str(row["Delivery Date Key"]) == "2016-06-14"
    assert row["WWI Salesperson ID"] == 3 and row["_valid"] is True
    context(spark, "2016-06-13 01:00:00", "2016-06-14 00:00:00")
    assert spark.sql(local_sale_query()).count() == 0
    context(spark, "2016-06-13 00:00:00", "2016-06-13 00:00:00")
    assert spark.sql(local_sale_query()).count() == 0
    context(spark, "2016-06-13 00:00:00", "2016-06-14 00:00:00")
    spark.createDataFrame([], "PackageTypeID INT, PackageTypeName STRING, _cast_valid BOOLEAN").createOrReplaceTempView("warehouse_package_types")
    rows = spark.sql(local_sale_query()).collect()
    assert len(rows) == 1 and rows[0]["_valid"] is False


def test_spark_cast_failure_and_multiset_parity(spark):
    from s11_sale.runtime import typed_projection

    raw = spark.createDataFrame([("bad", "D1", "file.csv", "now")], "Quantity STRING, _drop STRING, _file STRING, _ingested_at STRING")
    row = raw.selectExpr(*typed_projection(["Quantity"])).first()
    assert row["Quantity"] is None and row["_cast_valid"] is False
    left = spark.createDataFrame([("same",), ("same",)], "value STRING")
    right = spark.createDataFrame([("same",), ("different",)], "value STRING")
    assert left.exceptAll(right).count() == right.exceptAll(left).count() == 1


def test_golden_d0_d1_d2_reconstructed_sources_and_parity(spark):
    from pyspark.sql import functions as F
    from s11_sale.runtime import parity_frames, read_csv

    needed = {
        "sales_invoices": "InvoiceID CustomerID BillToCustomerID SalespersonPersonID InvoiceDate ConfirmedDeliveryTime LastEditedWhen",
        "sales_invoice_lines": "InvoiceLineID InvoiceID StockItemID PackageTypeID Description Quantity UnitPrice TaxRate TaxAmount LineProfit ExtendedPrice LastEditedWhen",
        "warehouse_stock_items": "StockItemID IsChillerStock",
        "warehouse_package_types": "PackageTypeID PackageTypeName",
        "sales_customers": "CustomerID DeliveryCityID",
    }
    states = {}
    for drop in ("D0", "D1", "D2"):
        source_failures = 0
        for source, (table, key) in SOURCES.items():
            path = f"inputs/{drop}/WideWorldImporters.{source}.csv"
            entry = MANIFEST["files"][path]
            up = (read_csv(spark, str(ROOT / "golden"), path, entry["columns"])
                  .withColumn("_drop", F.lit(drop)).withColumn("_file", F.lit(path))
                  .withColumn("_ingested_at", F.current_timestamp())
                  .selectExpr(*typed_projection(entry["columns"]))
                  .select(*needed[table].split(), "_cast_valid"))
            if drop == "D0":
                state = up
            else:
                state = states[table]
                deleted = path.removesuffix(".csv") + ".deleted.csv"
                if deleted in MANIFEST["files"]:
                    gone = read_csv(spark, str(ROOT / "golden"), deleted, [key]).selectExpr(f"CAST({key} AS INT) AS {key}")
                    state = state.join(gone, key, "left_anti")
                state = state.join(up.select(key), key, "left_anti").unionByName(up)
            state = state.localCheckpoint(eager=True)
            if table in states:
                states[table].unpersist()
            states[table] = state
            state.createOrReplaceTempView(table)
            source_failures += state.where(f"NOT _cast_valid OR {key} IS NULL").count()
        last, new = drop_window(MANIFEST, drop)
        context(spark, last, new)
        candidates = spark.sql(local_sale_query()).cache()
        quarantine_count = source_failures + candidates.where("NOT _valid").count()
        assert quarantine_count == 0
        actual = candidates.select(*[c for c, _ in COLUMNS])
        left, right = parity_frames(spark, str(ROOT / "golden"), MANIFEST, drop, actual)
        result = assert_parity(left.count(), right.count(), left.exceptAll(right).count(),
                               right.exceptAll(left).count(), quarantine_count)
        print(f"S11 golden {drop}: {json.dumps(result, sort_keys=True)}")
        assert result["status"] == "PASS"
        if drop == "D0":
            assert result["actual_rows"] == 0
        candidates.unpersist()
    for state in states.values():
        state.unpersist()
