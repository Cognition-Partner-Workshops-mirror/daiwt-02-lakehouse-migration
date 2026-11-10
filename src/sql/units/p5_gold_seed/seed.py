"""Reusable D0 baseline seed; does not generate or extend golden values."""

from parity.parity_check import literal
from sql.framework import fmt_datetime7, identifier, relation


INTEGER_COLUMNS = set("""latest_recorded_population day_number calendar_month_number
calendar_year fiscal_month_number fiscal_year iso_week_number lead_time_days
quantity_per_outer payment_days quantity ordered_outers ordered_quantity received_outers
total_dry_items total_chiller_items quantity_on_hand last_stocktake_quantity
reorder_level target_stock_level""".split())
DECIMALS = {name: 2 for name in """unit_price recommended_retail_price
total_excluding_tax tax_amount total_including_tax profit last_cost_price
outstanding_balance""".split()}
DECIMALS.update(tax_rate=3, typical_weight_per_unit=3)
DATETIMES = {"valid_from", "valid_to", "data_load_started", "data_load_completed",
             "source_system_cutoff_time", "cutoff_time"}


def golden_csv_source(entry, golden_path):
    schema_string = ", ".join(f"{identifier(c)} STRING" for c in entry["columns"])
    return (
        f"read_files({literal(golden_path.rstrip('/') + '/' + entry['path'])}, "
        f"format=>'csv', header=>true, schema=>{literal(schema_string)}, "
        f"nullValue=>{literal(chr(92) + 'N')}, multiLine=>true, escape=>{literal(chr(34))}, mode=>'FAILFAST')")


def column_type(column):
    if column == "date" or column.endswith("date_key"):
        return "DATE"
    if column in DECIMALS:
        return f"DECIMAL(18,{DECIMALS[column]})"
    if (column.endswith("_key") or column.startswith("wwi_") or
            column in INTEGER_COLUMNS or column.startswith("is_") or column == "was_successful"):
        return "BIGINT"
    return "STRING"


def seed_statements(contract, catalog, schema, golden_path, *, tables=None, replace=False):
    """Yield CTAS for all 16 tables or a G-unit's selected contract table names.

    Default preserves an existing table. replace=True explicitly restores D0.
    """
    entries = [e for e in contract["entries"] if e["drop"] == "D0" and e["kind"] != "report"]
    available = {e["table"] for e in entries}
    if tables is not None and not set(tables) <= available:
        raise ValueError("Unknown seed table")
    for entry in entries:
        if tables is not None and entry["table"] not in tables:
            continue
        target = relation(f"{catalog}.{schema}.{entry['table'].split('.')[1]}")
        columns = entry["columns"]
        projection = ", ".join(
            f"{fmt_datetime7(identifier(c)) if c in DATETIMES else f'CAST({identifier(c)} AS {column_type(c)})'} AS {identifier(c)}"
            for c in columns)
        mode = "OR REPLACE TABLE" if replace else "TABLE IF NOT EXISTS"
        yield (f"CREATE {mode} {target} USING DELTA AS SELECT {projection} "
               f"FROM {golden_csv_source(entry, golden_path)}")


def seed(spark, contract, catalog, schema, golden_path, **kwargs):
    for statement in seed_statements(contract, catalog, schema, golden_path, **kwargs):
        spark.sql(statement).collect()
