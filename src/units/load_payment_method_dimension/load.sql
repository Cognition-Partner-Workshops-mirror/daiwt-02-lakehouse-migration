CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_payment_method') AS
WITH versions AS (
    SELECT PaymentMethodID, PaymentMethodName, ValidFrom, 0 AS source_order
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.paymentmethods_archive')
    UNION ALL
    SELECT PaymentMethodID, PaymentMethodName, ValidFrom, 1 AS source_order
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.paymentmethods')
), staged AS (
    SELECT PaymentMethodID, PaymentMethodName, CAST(ValidFrom AS TIMESTAMP_NTZ) AS valid_from, source_order
    FROM versions
    WHERE ValidFrom > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY valid_from, source_order, PaymentMethodID) AS INT) AS payment_method_key,
    CAST(PaymentMethodID AS INT) AS wwi_payment_method_id,
    CAST(PaymentMethodName AS STRING) AS payment_method,
    CAST(valid_from AS TIMESTAMP_NTZ) AS valid_from,
    CAST(COALESCE(
        LEAD(valid_from) OVER (PARTITION BY PaymentMethodID ORDER BY valid_from),
        CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ)
    ) AS TIMESTAMP_NTZ) AS valid_to,
    CAST(4 AS INT) AS lineage_key
FROM staged
UNION ALL
SELECT
    CAST(0 AS INT) AS payment_method_key,
    CAST(0 AS INT) AS wwi_payment_method_id,
    CAST('Unknown' AS STRING) AS payment_method,
    CAST('2013-01-01 00:00:00' AS TIMESTAMP_NTZ) AS valid_from,
    CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ) AS valid_to,
    CAST(0 AS INT) AS lineage_key;
