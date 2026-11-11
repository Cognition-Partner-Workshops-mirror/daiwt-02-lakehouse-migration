CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.fact_sale') AS
WITH stock_item_history AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.stockitems')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.stockitems_archive')
), customer_history AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.customers')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.customers_archive')
), package_type_history AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.packagetypes')
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.packagetypes_archive')
), stock_items AS (
    SELECT * FROM stock_item_history
    WHERE ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
      AND ValidTo > CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), customers AS (
    SELECT * FROM customer_history
    WHERE ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
      AND ValidTo > CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), package_types AS (
    SELECT * FROM package_type_history
    WHERE ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
      AND ValidTo > CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), staged AS (
    SELECT i.InvoiceID, il.InvoiceLineID, i.InvoiceDate, i.ConfirmedDeliveryTime,
           i.CustomerID, i.BillToCustomerID, i.SalespersonPersonID, c.DeliveryCityID,
           il.StockItemID, il.Description, pt.PackageTypeName, il.Quantity,
           il.UnitPrice, il.TaxRate, il.ExtendedPrice, il.TaxAmount, il.LineProfit,
           si.IsChillerStock, GREATEST(i.LastEditedWhen, il.LastEditedWhen) AS lookup_ts
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.invoices') i
    INNER JOIN IDENTIFIER(:catalog || '.' || :bronze_schema || '.invoicelines') il
        ON i.InvoiceID = il.InvoiceID
    INNER JOIN stock_items si ON il.StockItemID = si.StockItemID
    INNER JOIN package_types pt ON il.PackageTypeID = pt.PackageTypeID
    INNER JOIN customers c ON i.CustomerID = c.CustomerID
    INNER JOIN customers bt ON i.BillToCustomerID = bt.CustomerID
    WHERE GREATEST(i.LastEditedWhen, il.LastEditedWhen) > CAST('2012-12-31' AS TIMESTAMP_NTZ)
      AND GREATEST(i.LastEditedWhen, il.LastEditedWhen) <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), city_lookup AS (
    SELECT s.InvoiceLineID, d.city_key AS dimension_key
    FROM staged s
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_city') d
        ON d.wwi_city_id = s.DeliveryCityID
       AND s.lookup_ts > CAST(d.valid_from AS TIMESTAMP_NTZ)
       AND s.lookup_ts <= CAST(d.valid_to AS TIMESTAMP_NTZ)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.InvoiceLineID ORDER BY d.valid_from, d.city_key) = 1
), customer_lookup AS (
    SELECT s.InvoiceLineID, d.customer_key AS dimension_key
    FROM staged s
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer') d
        ON d.wwi_customer_id = s.CustomerID
       AND s.lookup_ts > CAST(d.valid_from AS TIMESTAMP_NTZ)
       AND s.lookup_ts <= CAST(d.valid_to AS TIMESTAMP_NTZ)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.InvoiceLineID ORDER BY d.valid_from, d.customer_key) = 1
), bill_to_customer_lookup AS (
    SELECT s.InvoiceLineID, d.customer_key AS dimension_key
    FROM staged s
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer') d
        ON d.wwi_customer_id = s.BillToCustomerID
       AND s.lookup_ts > CAST(d.valid_from AS TIMESTAMP_NTZ)
       AND s.lookup_ts <= CAST(d.valid_to AS TIMESTAMP_NTZ)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.InvoiceLineID ORDER BY d.valid_from, d.customer_key) = 1
), stock_item_lookup AS (
    SELECT s.InvoiceLineID, d.stock_item_key AS dimension_key
    FROM staged s
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_stock_item') d
        ON d.wwi_stock_item_id = s.StockItemID
       AND s.lookup_ts > CAST(d.valid_from AS TIMESTAMP_NTZ)
       AND s.lookup_ts <= CAST(d.valid_to AS TIMESTAMP_NTZ)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.InvoiceLineID ORDER BY d.valid_from, d.stock_item_key) = 1
), salesperson_lookup AS (
    SELECT s.InvoiceLineID, d.employee_key AS dimension_key
    FROM staged s
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_employee') d
        ON d.wwi_employee_id = s.SalespersonPersonID
       AND s.lookup_ts > CAST(d.valid_from AS TIMESTAMP_NTZ)
       AND s.lookup_ts <= CAST(d.valid_to AS TIMESTAMP_NTZ)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.InvoiceLineID ORDER BY d.valid_from, d.employee_key) = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY s.InvoiceID, s.InvoiceLineID) AS BIGINT) AS sale_key,
    CAST(COALESCE(city.dimension_key, 0) AS INT) AS city_key,
    CAST(COALESCE(customer.dimension_key, 0) AS INT) AS customer_key,
    CAST(COALESCE(bill_to_customer.dimension_key, 0) AS INT) AS bill_to_customer_key,
    CAST(COALESCE(stock_item.dimension_key, 0) AS INT) AS stock_item_key,
    CAST(s.InvoiceDate AS DATE) AS invoice_date_key,
    CAST(s.ConfirmedDeliveryTime AS DATE) AS delivery_date_key,
    CAST(COALESCE(salesperson.dimension_key, 0) AS INT) AS salesperson_key,
    CAST(s.InvoiceID AS INT) AS wwi_invoice_id,
    CAST(s.Description AS STRING) AS description,
    CAST(s.PackageTypeName AS STRING) AS package,
    CAST(s.Quantity AS INT) AS quantity,
    CAST(s.UnitPrice AS DECIMAL(18, 2)) AS unit_price,
    CAST(s.TaxRate AS DECIMAL(18, 3)) AS tax_rate,
    CAST(CAST(s.ExtendedPrice AS DECIMAL(18, 2)) - CAST(s.TaxAmount AS DECIMAL(18, 2)) AS DECIMAL(18, 2)) AS total_excluding_tax,
    CAST(s.TaxAmount AS DECIMAL(18, 2)) AS tax_amount,
    CAST(s.LineProfit AS DECIMAL(18, 2)) AS profit,
    CAST(s.ExtendedPrice AS DECIMAL(18, 2)) AS total_including_tax,
    CAST(CASE WHEN s.IsChillerStock = FALSE THEN s.Quantity ELSE 0 END AS INT) AS total_dry_items,
    CAST(CASE WHEN s.IsChillerStock <> FALSE THEN s.Quantity ELSE 0 END AS INT) AS total_chiller_items,
    CAST(11 AS INT) AS lineage_key
FROM staged s
LEFT JOIN city_lookup city ON s.InvoiceLineID = city.InvoiceLineID
LEFT JOIN customer_lookup customer ON s.InvoiceLineID = customer.InvoiceLineID
LEFT JOIN bill_to_customer_lookup bill_to_customer ON s.InvoiceLineID = bill_to_customer.InvoiceLineID
LEFT JOIN stock_item_lookup stock_item ON s.InvoiceLineID = stock_item.InvoiceLineID
LEFT JOIN salesperson_lookup salesperson ON s.InvoiceLineID = salesperson.InvoiceLineID;
