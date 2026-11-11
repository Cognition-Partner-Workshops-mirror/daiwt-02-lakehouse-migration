CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer') AS
WITH customer_history AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.customers')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.customers_archive')
), category_history AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.customercategories')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.customercategories_archive')
), buying_group_history AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.buyinggroups')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.buyinggroups_archive')
), people_history AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.people')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.people_archive')
), customers AS (
    SELECT * FROM customer_history
    WHERE ValidFrom <= CAST('2013-01-01' AS TIMESTAMP_NTZ)
      AND ValidTo > CAST('2013-01-01' AS TIMESTAMP_NTZ)
), categories AS (
    SELECT * FROM category_history
    WHERE ValidFrom <= CAST('2013-01-01' AS TIMESTAMP_NTZ)
      AND ValidTo > CAST('2013-01-01' AS TIMESTAMP_NTZ)
), buying_groups AS (
    SELECT * FROM buying_group_history
    WHERE ValidFrom <= CAST('2013-01-01' AS TIMESTAMP_NTZ)
      AND ValidTo > CAST('2013-01-01' AS TIMESTAMP_NTZ)
), people AS (
    SELECT * FROM people_history
    WHERE ValidFrom <= CAST('2013-01-01' AS TIMESTAMP_NTZ)
      AND ValidTo > CAST('2013-01-01' AS TIMESTAMP_NTZ)
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY c.CustomerID) AS INT) AS customer_key,
    CAST(c.CustomerID AS INT) AS wwi_customer_id,
    CAST(c.CustomerName AS STRING) AS customer,
    CAST(bt.CustomerName AS STRING) AS bill_to_customer,
    CAST(cc.CustomerCategoryName AS STRING) AS category,
    CAST(COALESCE(bg.BuyingGroupName, 'N/A') AS STRING) AS buying_group,
    CAST(p.FullName AS STRING) AS primary_contact,
    CAST(c.DeliveryPostalCode AS STRING) AS postal_code,
    CAST('2013-01-01 00:00:00.0000000' AS TIMESTAMP_NTZ) AS valid_from,
    CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ) AS valid_to,
    CAST(2 AS INT) AS lineage_key
FROM customers c
INNER JOIN customers bt ON c.BillToCustomerID = bt.CustomerID
INNER JOIN categories cc ON c.CustomerCategoryID = cc.CustomerCategoryID
INNER JOIN people p ON c.PrimaryContactPersonID = p.PersonID
LEFT JOIN buying_groups bg ON c.BuyingGroupID = bg.BuyingGroupID
WHERE c.BuyingGroupID IS NOT NULL
UNION ALL
SELECT
    CAST(0 AS INT), CAST(0 AS INT),
    CAST('Unknown' AS STRING), CAST('N/A' AS STRING),
    CAST('N/A' AS STRING), CAST('N/A' AS STRING),
    CAST('N/A' AS STRING), CAST('N/A' AS STRING),
    CAST('2013-01-01 00:00:00.0000000' AS TIMESTAMP_NTZ),
    CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ),
    CAST(0 AS INT);
