"""S05's local temporal adapter over P4's typed bronze contract."""
import re

SENTINEL = "9999-12-31 23:59:59.9999999"
SOURCES = {
    "StockItems": ("warehouse_stock_items", "StockItemID"),
    "StockItems_Archive": ("warehouse_stock_items_archive", None),
    "Colors": ("warehouse_colors", "ColorID"),
    "Colors_Archive": ("warehouse_colors_archive", None),
    "PackageTypes": ("warehouse_package_types", "PackageTypeID"),
    "PackageTypes_Archive": ("warehouse_package_types_archive", None),
}
COLUMNS = [
    "wwi_stock_item_id", "stock_item", "color", "selling_package", "buying_package",
    "brand", "size", "lead_time_days", "quantity_per_outer", "is_chiller_stock",
    "barcode", "tax_rate", "unit_price", "recommended_retail_price",
    "typical_weight_per_unit", "photo", "valid_from", "valid_to",
]
TYPES = [
    "INT", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING",
    "INT", "INT", "BOOLEAN", "STRING", "DECIMAL(18,3)", "DECIMAL(18,2)",
    "DECIMAL(18,2)", "DECIMAL(18,3)", "BINARY", "STRING", "STRING",
]
RULES = {
    "identity": "wwi_stock_item_id IS NOT NULL AND valid_from IS NOT NULL",
    "packages": "selling_package IS NOT NULL AND buying_package IS NOT NULL",
    "interval": "valid_from < valid_to",
    "nonnegative": "tax_rate >= 0 AND unit_price >= 0 AND recommended_retail_price >= 0 AND typical_weight_per_unit >= 0",
    "chiller": "is_chiller_stock IS NOT NULL",
}


def datetime7(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,7})?", value
    ):
        raise ValueError(f"Invalid datetime2(7): {value!r}")
    whole, _, fraction = value.partition(".")
    return whole + "." + fraction.ljust(7, "0")


def window(capture, drop):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError(f"Unsupported drop: {drop}")
    def cutoff(value):
        if isinstance(value, list):
            return next(r["Cutoff Time"] for r in value if r["Table Name"] == "Stock Item")
        return value
    metadata = capture["drop"]
    last = "1900-01-01 00:00:00" if drop == "D0" else cutoff(metadata["last_cutoff"])
    result = (datetime7(last), datetime7(cutoff(metadata["new_cutoff"])))
    if result[0] >= result[1]:
        raise ValueError("Empty or inverted extraction window")
    return result


def changes_sql(bronze, silver):
    # Fixed-width strings preserve SQL Server's seventh fractional digit, including year 9999.
    return f"""
WITH items AS (
 SELECT * FROM {bronze}.warehouse_stock_items
 UNION ALL SELECT * FROM {bronze}.warehouse_stock_items_archive
), packages AS (
 SELECT * FROM {bronze}.warehouse_package_types
 UNION ALL SELECT * FROM {bronze}.warehouse_package_types_archive
), colors AS (
 SELECT * FROM {bronze}.warehouse_colors
 UNION ALL SELECT * FROM {bronze}.warehouse_colors_archive
), events AS (
 SELECT i.StockItemID, i.ValidFrom_str AS event_time FROM items i
 CROSS JOIN {silver}.stock_item_window w
 WHERE i.ValidFrom_str > w.last_cutoff AND i.ValidFrom_str <= w.new_cutoff
), staged AS (
 SELECT CAST(i.StockItemID AS INT) AS wwi_stock_item_id,
 i.StockItemName AS stock_item, COALESCE(c.ColorName, 'N/A') AS color,
 s.PackageTypeName AS selling_package, b.PackageTypeName AS buying_package,
 COALESCE(i.Brand, 'N/A') AS brand, COALESCE(i.Size, 'N/A') AS size,
 CAST(i.LeadTimeDays AS INT) AS lead_time_days,
 CAST(i.QuantityPerOuter AS INT) AS quantity_per_outer,
 CAST(i.IsChillerStock AS BOOLEAN) AS is_chiller_stock,
 COALESCE(i.Barcode, 'N/A') AS barcode,
 CAST(i.TaxRate AS DECIMAL(18,3)) AS tax_rate,
 CAST(i.UnitPrice AS DECIMAL(18,2)) AS unit_price,
 CAST(i.RecommendedRetailPrice AS DECIMAL(18,2)) AS recommended_retail_price,
 CAST(i.TypicalWeightPerUnit AS DECIMAL(18,3)) AS typical_weight_per_unit,
 unhex(i.Photo) AS photo, i.ValidFrom_str AS valid_from
 FROM events e JOIN items i ON e.StockItemID = i.StockItemID
 AND i.ValidFrom_str <= e.event_time AND e.event_time < i.ValidTo_str
 JOIN packages s ON i.UnitPackageID = s.PackageTypeID
 AND s.ValidFrom_str <= e.event_time AND e.event_time < s.ValidTo_str
 JOIN packages b ON i.OuterPackageID = b.PackageTypeID
 AND b.ValidFrom_str <= e.event_time AND e.event_time < b.ValidTo_str
 LEFT JOIN colors c ON i.ColorID = c.ColorID
 AND c.ValidFrom_str <= e.event_time AND e.event_time < c.ValidTo_str
), times AS (
 SELECT DISTINCT wwi_stock_item_id, valid_from FROM staged
), intervals AS (
 SELECT *, LEAD(valid_from) OVER (PARTITION BY wwi_stock_item_id ORDER BY valid_from) AS next_time
 FROM times
)
SELECT s.*, COALESCE(t.next_time, '{SENTINEL}') AS valid_to
FROM staged s JOIN intervals t
ON s.wwi_stock_item_id = t.wwi_stock_item_id AND s.valid_from = t.valid_from
"""
