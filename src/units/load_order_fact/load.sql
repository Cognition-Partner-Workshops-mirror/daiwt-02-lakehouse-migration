CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.fact_order') AS
WITH staged AS (
    SELECT
        o.OrderID, ol.OrderLineID, o.BackorderOrderID, o.OrderDate,
        ol.PickingCompletedWhen, ol.Description, pt.PackageTypeName,
        ol.Quantity, CAST(ol.UnitPrice AS DECIMAL(18,2)) AS unit_price,
        CAST(ol.TaxRate AS DECIMAL(18,3)) AS tax_rate,
        c.DeliveryCityID, o.CustomerID, ol.StockItemID,
        o.SalespersonPersonID, o.PickedByPersonID,
        GREATEST(CAST(o.LastEditedWhen AS TIMESTAMP_NTZ),
                 CAST(ol.LastEditedWhen AS TIMESTAMP_NTZ)) AS lookup_ts
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.orders') AS o
    INNER JOIN IDENTIFIER(:catalog || '.' || :bronze_schema || '.orderlines') AS ol
        ON o.OrderID = ol.OrderID
    INNER JOIN IDENTIFIER(:catalog || '.' || :bronze_schema || '.packagetypes') AS pt
        ON ol.PackageTypeID = pt.PackageTypeID
    INNER JOIN IDENTIFIER(:catalog || '.' || :bronze_schema || '.customers') AS c
        ON o.CustomerID = c.CustomerID
), amounts AS (
    SELECT *,
        CAST(ROUND(CAST(Quantity AS DECIMAL(10,0)) * unit_price, 2) AS DECIMAL(18,2)) AS total_excluding_tax,
        CAST(ROUND(CAST(Quantity AS DECIMAL(10,0)) * unit_price * tax_rate
                   * CAST(0.01 AS DECIMAL(3,2)), 2) AS DECIMAL(18,2)) AS tax_amount
    FROM staged
    WHERE lookup_ts > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND lookup_ts <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), resolved AS (
    SELECT a.*, ci.city_key, cu.customer_key, si.stock_item_key,
        sp.employee_key AS salesperson_key, pk.employee_key AS picker_key
    FROM amounts AS a
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_city') AS ci
        ON a.DeliveryCityID = ci.wwi_city_id
        AND a.lookup_ts > ci.valid_from AND a.lookup_ts <= ci.valid_to
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer') AS cu
        ON a.CustomerID = cu.wwi_customer_id
        AND a.lookup_ts > cu.valid_from AND a.lookup_ts <= cu.valid_to
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_stock_item') AS si
        ON a.StockItemID = si.wwi_stock_item_id
        AND a.lookup_ts > si.valid_from AND a.lookup_ts <= si.valid_to
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_employee') AS sp
        ON a.SalespersonPersonID = sp.wwi_employee_id
        AND a.lookup_ts > sp.valid_from AND a.lookup_ts <= sp.valid_to
    LEFT JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_employee') AS pk
        ON a.PickedByPersonID = pk.wwi_employee_id
        AND a.lookup_ts > pk.valid_from AND a.lookup_ts <= pk.valid_to
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY a.OrderLineID
        ORDER BY ci.valid_from, cu.valid_from, si.valid_from, sp.valid_from, pk.valid_from
    ) = 1
)
SELECT
    CAST(ROW_NUMBER() OVER (ORDER BY OrderID, OrderLineID) AS BIGINT) AS order_key,
    CAST(COALESCE(city_key, 0) AS INT) AS city_key,
    CAST(COALESCE(customer_key, 0) AS INT) AS customer_key,
    CAST(COALESCE(stock_item_key, 0) AS INT) AS stock_item_key,
    CAST(OrderDate AS DATE) AS order_date_key,
    CAST(PickingCompletedWhen AS DATE) AS picked_date_key,
    CAST(COALESCE(salesperson_key, 0) AS INT) AS salesperson_key,
    CAST(COALESCE(picker_key, 0) AS INT) AS picker_key,
    CAST(OrderID AS INT) AS wwi_order_id,
    CAST(BackorderOrderID AS INT) AS wwi_backorder_id,
    CAST(Description AS STRING) AS description,
    CAST(PackageTypeName AS STRING) AS package,
    CAST(Quantity AS INT) AS quantity,
    CAST(unit_price AS DECIMAL(18,2)) AS unit_price,
    CAST(tax_rate AS DECIMAL(18,3)) AS tax_rate,
    CAST(total_excluding_tax AS DECIMAL(18,2)) AS total_excluding_tax,
    CAST(tax_amount AS DECIMAL(18,2)) AS tax_amount,
    CAST(total_excluding_tax + tax_amount AS DECIMAL(18,2)) AS total_including_tax,
    CAST(9 AS INT) AS lineage_key
FROM resolved;
