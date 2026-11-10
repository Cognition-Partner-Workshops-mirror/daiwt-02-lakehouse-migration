"""S11 contracts shared by the replay notebooks and declarative pipeline."""
import re
from datetime import datetime


SOURCES = {
    "Sales.Invoices": ("sales_invoices", "InvoiceID"),
    "Sales.InvoiceLines": ("sales_invoice_lines", "InvoiceLineID"),
    "Warehouse.StockItems": ("warehouse_stock_items", "StockItemID"),
    "Warehouse.PackageTypes": ("warehouse_package_types", "PackageTypeID"),
    "Sales.Customers": ("sales_customers", "CustomerID"),
}

COLUMNS = [
    ("Invoice Date Key", "DATE"), ("Delivery Date Key", "DATE"),
    ("WWI Invoice ID", "INT"), ("Description", "STRING"), ("Package", "STRING"),
    ("Quantity", "INT"), ("Unit Price", "DECIMAL(18,2)"),
    ("Tax Rate", "DECIMAL(18,3)"), ("Total Excluding Tax", "DECIMAL(18,2)"),
    ("Tax Amount", "DECIMAL(18,2)"), ("Profit", "DECIMAL(18,2)"),
    ("Total Including Tax", "DECIMAL(18,2)"), ("Total Dry Items", "INT"),
    ("Total Chiller Items", "INT"), ("WWI City ID", "INT"),
    ("WWI Customer ID", "INT"), ("WWI Bill To Customer ID", "INT"),
    ("WWI Stock Item ID", "INT"), ("WWI Salesperson ID", "INT"),
    ("Last Modified When", "TIMESTAMP_NTZ"),
]
SHARED = [name for name, _ in COLUMNS[:14]]
ID_DIMENSIONS = [
    ("WWI City ID", "city_key", "Dimension.City", "wwi_city_id"),
    ("WWI Customer ID", "customer_key", "Dimension.Customer", "wwi_customer_id"),
    ("WWI Bill To Customer ID", "bill_to_customer_key", "Dimension.Customer", "wwi_customer_id"),
    ("WWI Stock Item ID", "stock_item_key", "Dimension.Stock_Item", "wwi_stock_item_id"),
    ("WWI Salesperson ID", "salesperson_key", "Dimension.Employee", "wwi_employee_id"),
]


def quote(name):
    return "`" + name.replace("`", "``") + "`"


def namespace(catalog, schema):
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", x) for x in (catalog, schema)):
        raise ValueError("Invalid catalog/schema")
    return f"{quote(catalog)}.{quote(schema)}"


def target_namespace(catalog, schema, layer):
    if layer not in ("bronze", "silver") or not (
        (catalog == "daiwt_02_mig_prod" and schema == layer)
        or (catalog == "daiwt_02_mig_dev" and re.fullmatch(layer + r"_[A-Za-z_0-9]+", schema))
    ):
        raise ValueError("Mismatched target catalog/schema")
    return namespace(catalog, schema)


def cutoff(value):
    parsed = datetime.fromisoformat(value[:26])
    return parsed.strftime("%Y-%m-%d %H:%M:%S.%f") + (value[26:27] or "0")


def drop_window(manifest, drop, last="", new=""):
    entry = next(d for d in manifest["drops"] if d["name"] == drop)

    def sale_value(value):
        return value if isinstance(value, str) else next(
            r["Cutoff Time"] for r in value if r["Table Name"] == "Sale")

    expected = tuple(cutoff(sale_value(entry[key])) for key in ("last_cutoff", "new_cutoff"))
    supplied = (cutoff(last) if last else expected[0], cutoff(new) if new else expected[1])
    if supplied != expected:
        raise ValueError(f"Cutoffs differ from manifest Sale window for {drop}")
    return supplied


def sale_query(bronze):
    expressions = [
        "CAST(i.InvoiceDate AS DATE)", "CAST(i.ConfirmedDeliveryTime AS DATE)",
        "i.InvoiceID", "il.Description", "pt.PackageTypeName", "il.Quantity",
        "il.UnitPrice", "il.TaxRate", "il.ExtendedPrice - il.TaxAmount", "il.TaxAmount",
        "il.LineProfit", "il.ExtendedPrice",
        "CASE WHEN si.IsChillerStock = false THEN il.Quantity ELSE 0 END",
        "CASE WHEN si.IsChillerStock = true THEN il.Quantity ELSE 0 END",
        "c.DeliveryCityID", "i.CustomerID", "i.BillToCustomerID", "il.StockItemID",
        "i.SalespersonPersonID", "CASE WHEN il.LastEditedWhen > i.LastEditedWhen THEN il.LastEditedWhen ELSE i.LastEditedWhen END",
    ]
    projection = ",\n".join(f"CAST({expr} AS {typ}) AS {quote(name)}"
                            for expr, (name, typ) in zip(expressions, COLUMNS))
    valid = " AND ".join([
        "i.InvoiceID IS NOT NULL", "il.InvoiceLineID IS NOT NULL",
        "si.StockItemID IS NOT NULL", "pt.PackageTypeID IS NOT NULL",
        "pt.PackageTypeName IS NOT NULL",
        "c.CustomerID IS NOT NULL", "bt.CustomerID IS NOT NULL",
        "si.IsChillerStock IS NOT NULL",
        "i.LastEditedWhen IS NOT NULL", "il.LastEditedWhen IS NOT NULL",
        "il.Quantity IS NOT NULL", "il.TaxRate IS NOT NULL", "il.TaxAmount IS NOT NULL",
        "il.LineProfit IS NOT NULL", "il.ExtendedPrice IS NOT NULL",
    ])
    return f"""
      WITH candidates AS (
        SELECT {projection}, il.InvoiceLineID AS _line_id,
               COALESCE({valid}, false) AS _valid
        FROM {bronze}.sales_invoice_lines il
        LEFT JOIN {bronze}.sales_invoices i ON i.InvoiceID = il.InvoiceID
        LEFT JOIN {bronze}.warehouse_stock_items si ON si.StockItemID = il.StockItemID
        LEFT JOIN {bronze}.warehouse_package_types pt ON pt.PackageTypeID = il.PackageTypeID
        LEFT JOIN {bronze}.sales_customers c ON c.CustomerID = i.CustomerID
        LEFT JOIN {bronze}.sales_customers bt ON bt.CustomerID = i.BillToCustomerID
      )
      SELECT c.* FROM candidates c CROSS JOIN (
        SELECT last_cutoff, new_cutoff FROM {bronze}.golden_drops
        WHERE table_name = 'Sale' AND drop_index = (
          SELECT max(drop_index) FROM {bronze}.bronze_replay_log)
      ) r
      WHERE (`Last Modified When` > CAST(r.last_cutoff AS TIMESTAMP_NTZ)
         AND `Last Modified When` <= CAST(r.new_cutoff AS TIMESTAMP_NTZ))
         OR (`Last Modified When` IS NULL AND NOT _valid AND r.last_cutoff <> r.new_cutoff)
      ORDER BY `WWI Invoice ID`, _line_id
    """


def assert_parity(actual_count, expected_count, extra, missing, quarantine):
    result = {"actual_rows": actual_count, "expected_rows": expected_count,
              "extra_rows": extra, "missing_rows": missing, "quarantine_rows": quarantine}
    result["status"] = "PASS" if actual_count == expected_count and not (extra or missing or quarantine) else "FAIL"
    return result
