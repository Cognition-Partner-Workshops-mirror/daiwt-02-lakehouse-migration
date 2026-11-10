"""Movement projection and multiset parity contract, usable without Spark."""
from collections import Counter
from datetime import datetime
from decimal import Decimal

PROJECTION = {
    "date_key": "CAST(TransactionOccurredWhen AS DATE)",
    "wwi_stock_item_transaction_id": "StockItemTransactionID",
    "wwi_invoice_id": "InvoiceID", "wwi_purchase_order_id": "PurchaseOrderID",
    "quantity": "try_cast(Quantity AS INT)", "wwi_stock_item_id": "StockItemID",
    "wwi_customer_id": "CustomerID", "wwi_supplier_id": "SupplierID",
    "wwi_transaction_type_id": "TransactionTypeID",
    "last_modified_when": "TransactionOccurredWhen",
}
SHARED = ("wwi_stock_item_transaction_id", "wwi_invoice_id",
          "wwi_purchase_order_id", "quantity", "date_key")
EXPECTED = {"D0": 236667, "D1": 1462, "D2": 1854}
VALID = """StockItemTransactionID IS NOT NULL AND StockItemID IS NOT NULL
AND TransactionTypeID IS NOT NULL AND TransactionOccurredWhen IS NOT NULL
AND Quantity IS NOT NULL AND LastEditedWhen IS NOT NULL AND NOT _bad_cast"""


def in_window(edited, drop, last, new):
    return edited is not None and edited <= new and (drop == "D0" or edited > last)


def parse_timestamp(value):
    return datetime.fromisoformat(value[:26])


def project(row):
    """Local equivalent of the typed projection; invalid casts raise."""
    def integer(name):
        value = row[name]
        if value in (None, "\\N"):
            return None
        result = int(Decimal(value))
        if not -(2**31) <= result < 2**31:
            raise ValueError("integer overflow")
        return result
    occurred = parse_timestamp(row["TransactionOccurredWhen"])
    return dict(zip(PROJECTION, [occurred.date(), integer("StockItemTransactionID"),
        integer("InvoiceID"), integer("PurchaseOrderID"), integer("Quantity"),
        integer("StockItemID"), integer("CustomerID"), integer("SupplierID"),
        integer("TransactionTypeID"), occurred]))


def assert_parity(staged, current, previous=(), expected_count=None, quarantine=0):
    actual, golden, prior = Counter(staged), Counter(current), Counter(previous)
    expected = golden - prior
    if quarantine or actual != expected or (expected_count is not None and sum(actual.values()) != expected_count):
        raise AssertionError(f"Movement parity mismatch: staged={sum(actual.values())}, "
                             f"expected={sum(expected.values())}, "
                             f"missing={sum((expected-actual).values())}, "
                             f"extra={sum((actual-expected).values())}, quarantine={quarantine}")
