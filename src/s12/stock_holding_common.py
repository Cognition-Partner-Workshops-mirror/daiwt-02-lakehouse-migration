"""S12 snapshot replay and golden projection contracts."""
import csv
import io
import re
from collections import Counter
from decimal import Decimal

SOURCE = {
    "StockItemID": ("wwi_stock_item_id", "INT"),
    "QuantityOnHand": ("quantity_on_hand", "INT"),
    "BinLocation": ("bin_location", "STRING"),
    "LastStocktakeQuantity": ("last_stocktake_quantity", "INT"),
    "LastCostPrice": ("last_cost_price", "DECIMAL(18,2)"),
    "ReorderLevel": ("reorder_level", "INT"),
    "TargetStockLevel": ("target_stock_level", "INT"),
    "LastEditedBy": ("last_edited_by", "INT"),
    "LastEditedWhen": ("last_edited_when", "TIMESTAMP_NTZ"),
}
SILVER = ["quantity_on_hand", "bin_location", "last_stocktake_quantity",
          "last_cost_price", "reorder_level", "target_stock_level", "wwi_stock_item_id"]
PARITY = ["wwi_stock_item_id"] + SILVER[:-1]


def drops_through(drop):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError("drop must be D0, D1 or D2")
    return [f"D{i}" for i in range(int(drop[1]) + 1)]


def verification_drops(landed, drop):
    requested = drops_through(drop)
    missing = set(requested) - set(landed)
    if missing:
        raise ValueError(f"Missing requested drops: {missing}")
    return requested


def qualified(catalog, schema, table):
    parts = (catalog, schema, table)
    if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError("Invalid table identifier")
    return ".".join(f"`{p}`" for p in parts)


def parse_csv(content, deleted=False):
    reader = csv.DictReader(io.StringIO(content))
    required = {"StockItemID"} if deleted else set(SOURCE)
    if not required.issubset(reader.fieldnames or []):
        raise ValueError("Missing source CSV columns")
    rows = []
    for row in reader:
        if None in row or any(v is None for v in row.values()):
            raise ValueError("CSV row width differs from header")
        rows.append({k: None if v == "\\N" else v for k, v in row.items()})
    return rows


def replay(state, upserts, deletes=()):
    result = dict(state)
    for row in deletes:
        result.pop(row["StockItemID"], None)
    for row in upserts:
        result[row["StockItemID"]] = row
    return result


def parity_price_type(drop):
    drops_through(drop)
    # D0 baseline uses the native result-set type.
    return "INT" if drop == "D0" else "DECIMAL(18,2)"


def parity_price(value, drop):
    price_type = parity_price_type(drop)
    if value is None:
        return None
    price = Decimal(str(value))
    if price_type == "INT":
        price = Decimal(int(price))
    return format(price, ".2f")


def compare_rows(actual, expected):
    actual, expected = Counter(map(tuple, actual)), Counter(map(tuple, expected))
    return {"actual_rows": sum(actual.values()), "golden_rows": sum(expected.values()),
            "actual_only": sum((actual - expected).values()),
            "golden_only": sum((expected - actual).values())}


def snapshot_query(raw, replay_log):
    casts = ", ".join(f"try_cast(`{src}` AS {typ}) AS {dst}"
                      for src, (dst, typ) in SOURCE.items() if dst in SILVER)
    castable = " AND ".join(
        f"(`{src}` IS NULL OR try_cast(`{src}` AS {typ}) IS NOT NULL)"
        for src, (dst, typ) in SOURCE.items() if dst in SILVER and typ != "STRING")
    return f"""
      WITH drops AS (
        SELECT DISTINCT `drop` AS _drop FROM {replay_log}
        WHERE source_table = 'Warehouse.StockItemHoldings' AND status = 'PASS'
      ),
      events AS (
        SELECT *, count(*) OVER (PARTITION BY _drop, StockItemID,
          CASE WHEN _op = 'delete' THEN 0 ELSE 1 END) AS _source_key_count
        FROM {raw}
      ),
      ranked AS (
        SELECT l.*, d._drop AS snapshot_drop,
          dense_rank() OVER (PARTITION BY d._drop, l.StockItemID
            ORDER BY l._drop DESC, CASE WHEN l._op = 'delete' THEN 0 ELSE 1 END DESC) AS rn
        FROM drops d JOIN events l ON l._drop <= d._drop
      )
      SELECT {casts}, snapshot_drop AS `drop`, ({castable}) AS _castable, _source_key_count
      FROM ranked WHERE rn = 1 AND _op <> 'delete'
    """


RULES = {
    "non_null_id": "wwi_stock_item_id IS NOT NULL",
    "unique_drop_id": "_key_count = 1 AND _source_key_count = 1",
    "non_negative": " AND ".join(f"{c} >= 0" for c in SILVER if c not in ("bin_location", "wwi_stock_item_id")),
    "valid_bin": "bin_location IS NOT NULL AND length(bin_location) <= 20",
    "castability": "_castable",
}
