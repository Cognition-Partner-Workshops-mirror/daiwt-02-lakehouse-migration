CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.fact_stock_holding') AS
WITH ranked_stock_items AS (
  SELECT wwi_stock_item_id, stock_item_key,
         ROW_NUMBER() OVER (PARTITION BY wwi_stock_item_id ORDER BY valid_to DESC) AS version_rank
  FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_stock_item')
)
SELECT
  CAST(ROW_NUMBER() OVER (ORDER BY h.StockItemID) AS BIGINT) AS stock_holding_key,
  CAST(COALESCE(d.stock_item_key, 0) AS INT) AS stock_item_key,
  CAST(h.QuantityOnHand AS INT) AS quantity_on_hand,
  CAST(h.BinLocation AS STRING) AS bin_location,
  CAST(h.LastStocktakeQuantity AS INT) AS last_stocktake_quantity,
  CAST(CASE WHEN h.LastCostPrice < 0 THEN CEIL(h.LastCostPrice)
            ELSE FLOOR(h.LastCostPrice) END AS DECIMAL(18,2)) AS last_cost_price,
  CAST(h.ReorderLevel AS INT) AS reorder_level,
  CAST(h.TargetStockLevel AS INT) AS target_stock_level,
  CAST(12 AS INT) AS lineage_key
FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.stockitemholdings') h
LEFT JOIN ranked_stock_items d
  ON d.wwi_stock_item_id = h.StockItemID AND d.version_rank = 1
WHERE h._drop = :drop;
