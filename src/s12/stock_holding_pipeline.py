import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from stock_holding_common import RULES, SILVER, qualified, snapshot_query


@dlt.view
@dlt.expect("non_null_id", RULES["non_null_id"])
@dlt.expect("unique_drop_id", RULES["unique_drop_id"])
@dlt.expect("non_negative", RULES["non_negative"])
@dlt.expect("valid_bin", RULES["valid_bin"])
@dlt.expect("castability", RULES["castability"])
def stock_holding_checked():
    catalog, bronze = spark.conf.get("s12.catalog"), spark.conf.get("s12.bronze_schema")
    raw = qualified(catalog, bronze, "warehouse_stock_item_holdings_raw")
    replay_log = qualified(catalog, bronze, "bronze_replay_log")
    return spark.sql(snapshot_query(raw, replay_log)).withColumn(
        "_key_count", F.count("*").over(Window.partitionBy("drop", "wwi_stock_item_id")))


@dlt.table
def stock_holding_snapshot():
    return dlt.read("stock_holding_checked").select(*SILVER, "drop")


@dlt.table
def stock_holding_snapshot_quarantine():
    reasons = [F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
               for name, rule in RULES.items()]
    return (dlt.read("stock_holding_checked")
            .withColumn("reason", F.concat_ws(";", *reasons))
            .where(F.col("reason") != "").select(*SILVER, "drop", "reason"))
