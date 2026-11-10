"""Declarative S11 datasets. Sale joins use current state, not SYSTEM_TIME."""
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

sys.path.insert(0, spark.conf.get("sale.source_path"))
from wwi_sale_to_silver.sale import COLUMNS, SOURCES, namespace, quote, sale_query

bronze = namespace(spark.conf.get("sale.catalog"), spark.conf.get("sale.bronze_schema"))
properties = {"delta.columnMapping.mode": "name", "delta.feature.timestampNtz": "supported"}


@dp.temporary_view(name="sale_candidates")
@dp.expect("sale_source_keys_and_casts", "_valid")
def sale_candidates():
    return spark.sql(sale_query(bronze))


@dp.materialized_view(name="sale_changes", table_properties=properties,
                      comment="S11 GetSaleUpdates; current-state joins; native positional contract")
@dp.expect("sale_required_fields", "`WWI Invoice ID` IS NOT NULL AND `WWI Stock Item ID` IS NOT NULL AND `Package` IS NOT NULL AND `Quantity` IS NOT NULL AND `Last Modified When` IS NOT NULL")
def sale_changes():
    return (spark.read.table("sale_candidates").orderBy("WWI Invoice ID", "_line_id")
            .select(*[c for c, _ in COLUMNS]))


@dp.materialized_view(name="sale_changes_quarantine", table_properties=properties,
                      comment="S11 warning-only source/key/type failures; must be empty for golden")
def sale_changes_quarantine():
    failures = (spark.read.table("sale_candidates").where("NOT _valid")
                .select(*[c for c, _ in COLUMNS])
                .withColumn("_reason", F.lit("sale source key, FK or cast failure")))
    for table, key in SOURCES.values():
        invalid = spark.read.table(f"{bronze}.{table}").where(f"{quote(key)} IS NULL")
        invalid = invalid.selectExpr(*[f"CAST(NULL AS {typ}) AS {quote(c)}" for c, typ in COLUMNS])
        failures = failures.unionByName(invalid.withColumn("_reason", F.lit(f"invalid source: {table}")))
    return failures
