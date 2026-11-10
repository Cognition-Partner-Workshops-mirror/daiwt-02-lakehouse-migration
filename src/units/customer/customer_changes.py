from pyspark import pipelines as dp
from customer_logic import COLUMNS, extract_sql, window_sql

catalog = spark.conf.get("source_catalog")
bronze_schema = spark.conf.get("bronze_schema")
checks = {
    "key_present": "wwi_customer_id IS NOT NULL",
    "valid_from_present": "valid_from IS NOT NULL",
    "valid_interval": "valid_from < valid_to",
}


@dp.materialized_view(name="customer_changes", comment="Customer extract; staging contract: Buying Group NOT NULL")
@dp.expect_all(checks)
def customer_changes():
    return spark.sql(extract_sql(catalog, bronze_schema))


@dp.materialized_view(name="customer_changes_quarantine")
def customer_changes_quarantine():
    invalid = " OR ".join(f"NOT COALESCE(({rule}), false)" for rule in checks.values())
    invalid += " OR NOT COALESCE(valid_from > last_cutoff AND valid_from <= new_cutoff, false)"
    return (spark.read.table("customer_changes")
            .crossJoin(spark.sql(window_sql(catalog, bronze_schema)))
            .where(invalid).select(*COLUMNS))
