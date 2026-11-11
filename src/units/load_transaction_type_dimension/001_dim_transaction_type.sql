CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_transaction_type') AS
WITH versions AS (
    SELECT TransactionTypeID, TransactionTypeName, ValidFrom
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.transactiontypes')
    UNION ALL
    SELECT TransactionTypeID, TransactionTypeName, ValidFrom
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.transactiontypes_archive')
), staged AS (
    SELECT TransactionTypeID, TransactionTypeName, ValidFrom
    FROM versions
    WHERE CAST(ValidFrom AS TIMESTAMP_NTZ) > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND CAST(ValidFrom AS TIMESTAMP_NTZ) <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY ValidFrom, CAST(TransactionTypeID AS INT)) AS INT) AS transaction_type_key,
    CAST(TransactionTypeID AS INT) AS wwi_transaction_type_id,
    CAST(TransactionTypeName AS STRING) AS transaction_type,
    CAST(ValidFrom AS STRING) AS valid_from,
    CAST(COALESCE(
        LEAD(ValidFrom) OVER (PARTITION BY TransactionTypeID ORDER BY ValidFrom),
        '9999-12-31 23:59:59.9999999'
    ) AS STRING) AS valid_to,
    CAST(7 AS INT) AS lineage_key
FROM staged
UNION ALL
SELECT
    CAST(0 AS INT) AS transaction_type_key,
    CAST(0 AS INT) AS wwi_transaction_type_id,
    CAST('Unknown' AS STRING) AS transaction_type,
    CAST('2013-01-01 00:00:00.0000000' AS STRING) AS valid_from,
    CAST('9999-12-31 23:59:59.9999999' AS STRING) AS valid_to,
    CAST(0 AS INT) AS lineage_key;
