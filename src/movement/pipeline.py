from pyspark import pipelines as dp
from pyspark.sql import functions as F
from logic import PROJECTION, VALID

catalog = spark.conf.get("movement.catalog")
bronze = spark.conf.get("movement.bronze_schema")
silver = spark.conf.get("movement.silver_schema")


def candidates():
    source = spark.read.table(f"{catalog}.{bronze}.warehouse_stock_item_transactions")
    window = spark.read.table(f"{catalog}.{silver}.movement_window")
    return source.drop("_drop").crossJoin(window).where(
        "(LastEditedWhen <= new_cutoff AND (_drop = 'D0' OR LastEditedWhen > last_cutoff)) "
        "OR LastEditedWhen IS NULL")


def projected(frame):
    return frame.selectExpr(*[f"{expr} AS {name}" for name, expr in PROJECTION.items()],
                            "_drop", "last_cutoff", "new_cutoff", "_bad_cast", "LastEditedWhen AS _last_edited_when")


@dp.materialized_view(name="movement_changes")
@dp.expect("valid_movement", "wwi_stock_item_transaction_id IS NOT NULL AND "
           "wwi_stock_item_id IS NOT NULL AND wwi_transaction_type_id IS NOT NULL "
           "AND date_key IS NOT NULL AND quantity IS NOT NULL AND _last_edited_when IS NOT NULL AND NOT _bad_cast")
def movement_changes():
    return projected(candidates())


@dp.materialized_view(name="movement_changes_quarantine")
def movement_changes_quarantine():
    return projected(candidates().where(~F.expr(VALID)))
