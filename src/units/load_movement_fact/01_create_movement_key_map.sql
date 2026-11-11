CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.movement_key_map') (
  wwi_stock_item_transaction_id INT,
  movement_key BIGINT
);
