CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.report_sale_by_day') AS
SELECT
  invoice_date_key,
  CAST(COUNT(*) AS BIGINT) AS sale_rows,
  CAST(SUM(total_including_tax) AS DECIMAL(38, 2)) AS total_including_tax
FROM IDENTIFIER(:catalog || '.' || :gold_schema || '.fact_sale')
GROUP BY invoice_date_key
