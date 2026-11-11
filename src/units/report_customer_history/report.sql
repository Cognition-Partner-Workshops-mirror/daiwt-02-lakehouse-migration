CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.report_customer_history') AS
SELECT
    CAST(customer_key AS INT) AS customer_key,
    CAST(wwi_customer_id AS INT) AS wwi_customer_id,
    CAST(customer AS STRING) AS customer,
    CAST(valid_from AS TIMESTAMP_NTZ) AS valid_from,
    CAST(valid_to AS TIMESTAMP_NTZ) AS valid_to
FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer');
