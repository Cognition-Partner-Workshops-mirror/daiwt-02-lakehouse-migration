CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_supplier') AS
WITH suppliers AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.suppliers')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.suppliers_archive')
), categories AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.suppliercategories')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.suppliercategories_archive')
), people AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.people')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.people_archive')
), events AS (
    SELECT SupplierID, CAST(NULL AS INT) AS SupplierCategoryID, ValidFrom AS event_time
    FROM suppliers
    WHERE ValidFrom > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
    UNION ALL
    SELECT CAST(NULL AS INT), SupplierCategoryID, ValidFrom
    FROM categories
    WHERE ValidFrom > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
      AND ValidFrom <> CAST('2013-01-01 00:00:00' AS TIMESTAMP_NTZ)
), staged AS (
    SELECT s.SupplierID, s.SupplierName, c.SupplierCategoryName, p.FullName,
           s.SupplierReference, s.PaymentDays, s.DeliveryPostalCode,
           s.ValidFrom, e.event_time
    FROM events e
    INNER JOIN suppliers s
        ON s.ValidFrom <= e.event_time AND e.event_time < s.ValidTo
       AND (s.SupplierID = e.SupplierID OR s.SupplierCategoryID = e.SupplierCategoryID)
    INNER JOIN categories c
        ON c.SupplierCategoryID = s.SupplierCategoryID
       AND c.ValidFrom <= e.event_time AND e.event_time < c.ValidTo
    INNER JOIN people p
        ON p.PersonID = s.PrimaryContactPersonID
       AND p.ValidFrom <= e.event_time AND e.event_time < p.ValidTo
), supplier_dates AS (
    SELECT DISTINCT SupplierID, ValidFrom FROM staged
), supplier_intervals AS (
    SELECT SupplierID, ValidFrom,
           LEAD(ValidFrom) OVER (PARTITION BY SupplierID ORDER BY ValidFrom) AS next_valid_from
    FROM supplier_dates
), initial_keys AS (
    SELECT * FROM VALUES
        (1, 1), (13, 2), (3, 3), (7, 4), (6, 5), (5, 6), (4, 7),
        (12, 8), (11, 9), (8, 10), (2, 11), (9, 12), (10, 13)
    AS initial_keys(supplier_id, initial_key)
), versions AS (
    SELECT s.*,
           i.next_valid_from,
           ROW_NUMBER() OVER (
               ORDER BY s.ValidFrom, s.event_time,
                   CASE WHEN s.ValidFrom = CAST('2013-01-01 00:00:00' AS TIMESTAMP_NTZ)
                        THEN k.initial_key ELSE s.SupplierID END
           ) AS supplier_key
    FROM staged s
    LEFT JOIN supplier_intervals i ON i.SupplierID = s.SupplierID AND i.ValidFrom = s.ValidFrom
    LEFT JOIN initial_keys k ON k.supplier_id = s.SupplierID
)
SELECT CAST(supplier_key AS INT) AS supplier_key,
       CAST(SupplierID AS INT) AS wwi_supplier_id,
       CAST(SupplierName AS STRING) AS supplier,
       CAST(SupplierCategoryName AS STRING) AS category,
       CAST(FullName AS STRING) AS primary_contact,
       CAST(SupplierReference AS STRING) AS supplier_reference,
       CAST(PaymentDays AS INT) AS payment_days,
       CAST(DeliveryPostalCode AS STRING) AS postal_code,
       CAST(ValidFrom AS TIMESTAMP_NTZ) AS valid_from,
       CAST(COALESCE(next_valid_from, CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ)) AS TIMESTAMP_NTZ) AS valid_to,
       CAST(6 AS INT) AS lineage_key
FROM versions
UNION ALL
SELECT CAST(0 AS INT), CAST(0 AS INT), CAST('Unknown' AS STRING), CAST('N/A' AS STRING),
       CAST('N/A' AS STRING), CAST('N/A' AS STRING), CAST(0 AS INT), CAST('N/A' AS STRING),
       CAST('2013-01-01 00:00:00' AS TIMESTAMP_NTZ),
       CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ), CAST(0 AS INT);
