from pyspark import pipelines as dp
from customer_logic import extract_sql

last = spark.conf.get("last_cutoff")
new = spark.conf.get("new_cutoff")
checks = {
    "key_present": "wwi_customer_id IS NOT NULL",
    "valid_from_present": "valid_from IS NOT NULL",
    "valid_interval": "valid_from < valid_to",
    "within_window": f"valid_from > '{last}' AND valid_from <= '{new}'",
}


@dp.materialized_view(name="customer_changes", comment="Customer extract; staging contract: Buying Group NOT NULL")
@dp.expect_all(checks)
def customer_changes():
    return spark.sql(extract_sql(spark.conf.get("source_catalog"),
                                 spark.conf.get("bronze_schema"), last, new))


@dp.materialized_view(name="customer_changes_quarantine")
def customer_changes_quarantine():
    invalid = " OR ".join(f"NOT COALESCE(({rule}), false)" for rule in checks.values())
    return spark.read.table("customer_changes").where(invalid)
