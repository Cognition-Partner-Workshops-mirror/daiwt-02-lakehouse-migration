CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.fact_movement') AS
WITH transactions AS (
  SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.stockitemtransactions')
  WHERE _drop = :drop
    AND LastEditedWhen > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
    AND LastEditedWhen <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), stock_item_matches AS (
  SELECT t.StockItemTransactionID, d.stock_item_key,
         ROW_NUMBER() OVER (PARTITION BY t.StockItemTransactionID ORDER BY d.valid_from) AS match_rank
  FROM transactions t
  INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_stock_item') d
    ON t.StockItemID = d.wwi_stock_item_id
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) > d.valid_from
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) <= d.valid_to
), customer_matches AS (
  SELECT t.StockItemTransactionID, d.customer_key,
         ROW_NUMBER() OVER (PARTITION BY t.StockItemTransactionID ORDER BY d.valid_from) AS match_rank
  FROM transactions t
  INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer') d
    ON t.CustomerID = d.wwi_customer_id
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) > d.valid_from
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) <= d.valid_to
), supplier_matches AS (
  SELECT t.StockItemTransactionID, d.supplier_key,
         ROW_NUMBER() OVER (PARTITION BY t.StockItemTransactionID ORDER BY d.valid_from) AS match_rank
  FROM transactions t
  INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_supplier') d
    ON t.SupplierID = d.wwi_supplier_id
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) > d.valid_from
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) <= d.valid_to
), transaction_type_matches AS (
  SELECT t.StockItemTransactionID, d.transaction_type_key,
         ROW_NUMBER() OVER (PARTITION BY t.StockItemTransactionID ORDER BY d.valid_from) AS match_rank
  FROM transactions t
  INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_transaction_type') d
    ON t.TransactionTypeID = d.wwi_transaction_type_id
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) > d.valid_from
    AND CAST(t.TransactionOccurredWhen AS TIMESTAMP_NTZ) <= d.valid_to
), mapped AS (
  SELECT t.*, k.movement_key
  FROM transactions t
  LEFT JOIN IDENTIFIER(:catalog || '.' || :gold_schema || '.movement_key_map') k ON t.StockItemTransactionID = k.wwi_stock_item_transaction_id
), keyed AS (
  SELECT * FROM mapped WHERE movement_key IS NOT NULL
  UNION ALL
  SELECT * EXCEPT (movement_key),
         (SELECT MAX(movement_key) FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.movement_key_map'))
           + ROW_NUMBER() OVER (ORDER BY StockItemTransactionID) AS movement_key
  FROM mapped WHERE movement_key IS NULL
)
SELECT
  CAST(t.movement_key AS BIGINT) AS movement_key,
  CAST(t.TransactionOccurredWhen AS DATE) AS date_key,
  CAST(COALESCE(si.stock_item_key, 0) AS INT) AS stock_item_key,
  CAST(COALESCE(c.customer_key, 0) AS INT) AS customer_key,
  CAST(COALESCE(s.supplier_key, 0) AS INT) AS supplier_key,
  CAST(COALESCE(tt.transaction_type_key, 0) AS INT) AS transaction_type_key,
  CAST(t.StockItemTransactionID AS INT) AS wwi_stock_item_transaction_id,
  CAST(t.InvoiceID AS INT) AS wwi_invoice_id,
  CAST(t.PurchaseOrderID AS INT) AS wwi_purchase_order_id,
  CAST(CASE WHEN t.Quantity < 0 THEN CEIL(t.Quantity) ELSE FLOOR(t.Quantity) END AS INT) AS quantity,
  CAST(8 AS INT) AS lineage_key
FROM keyed t
LEFT JOIN stock_item_matches si ON si.StockItemTransactionID = t.StockItemTransactionID AND si.match_rank = 1
LEFT JOIN customer_matches c ON c.StockItemTransactionID = t.StockItemTransactionID AND c.match_rank = 1
LEFT JOIN supplier_matches s ON s.StockItemTransactionID = t.StockItemTransactionID AND s.match_rank = 1
LEFT JOIN transaction_type_matches tt ON tt.StockItemTransactionID = t.StockItemTransactionID AND tt.match_rank = 1;
