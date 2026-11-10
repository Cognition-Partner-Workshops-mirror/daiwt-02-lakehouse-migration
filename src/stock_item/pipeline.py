"""Private temporal adapters until Platform P6 is available."""
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("s05.source_path"))
from model import RULES, changes_sql

bronze = f"`{spark.conf.get('s05.catalog')}`.`{spark.conf.get('s05.bronze_schema')}`"


@dp.materialized_view(name="stock_item_changes")
@dp.expect_all(RULES)
def stock_item_changes():
    return spark.sql(changes_sql(bronze))


@dp.materialized_view(name="stock_item_changes_quarantine")
def stock_item_changes_quarantine():
    invalid = " OR ".join(f"NOT COALESCE(({rule}), FALSE)" for rule in RULES.values())
    return spark.read.table("stock_item_changes").where(invalid)
