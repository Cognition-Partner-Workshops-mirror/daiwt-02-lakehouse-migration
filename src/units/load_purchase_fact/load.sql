CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.fact_purchase') AS
WITH stockitems AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.stockitems') WHERE _drop = :drop
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.stockitems_archive') WHERE _drop = :drop
), packagetypes AS (
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.packagetypes') WHERE _drop = :drop
    UNION ALL
    SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.packagetypes_archive') WHERE _drop = :drop
), staged AS (
    SELECT po.PurchaseOrderID, pol.PurchaseOrderLineID, po.OrderDate,
           po.SupplierID, pol.StockItemID, pol.OrderedOuters,
           pol.OrderedOuters * si.QuantityPerOuter AS ordered_quantity,
           pol.ReceivedOuters, pt.PackageTypeName, pol.IsOrderLineFinalized,
           GREATEST(po.LastEditedWhen, pol.LastEditedWhen) AS lookup_time
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.purchaseorders') po
    INNER JOIN IDENTIFIER(:catalog || '.' || :bronze_schema || '.purchaseorderlines') pol
        ON pol.PurchaseOrderID = po.PurchaseOrderID AND pol._drop = :drop
    INNER JOIN stockitems si ON si.StockItemID = pol.StockItemID
        AND si.ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
        AND CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ) < si.ValidTo
    INNER JOIN packagetypes pt ON pt.PackageTypeID = pol.PackageTypeID
        AND pt.ValidFrom <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
        AND CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ) < pt.ValidTo
    WHERE po._drop = :drop
      AND GREATEST(po.LastEditedWhen, pol.LastEditedWhen) > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND GREATEST(po.LastEditedWhen, pol.LastEditedWhen) <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), supplier_matches AS (
    SELECT p.PurchaseOrderLineID, s.supplier_key,
           ROW_NUMBER() OVER (PARTITION BY p.PurchaseOrderLineID ORDER BY s.valid_from) AS match_rank
    FROM staged p
    INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_supplier') s
        ON s.wwi_supplier_id = p.SupplierID
       AND p.lookup_time > s.valid_from AND p.lookup_time <= s.valid_to
), stock_item_matches AS (
    SELECT p.PurchaseOrderLineID, s.stock_item_key,
           ROW_NUMBER() OVER (PARTITION BY p.PurchaseOrderLineID ORDER BY s.valid_from) AS match_rank
    FROM staged p
    INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_stock_item') s
        ON s.wwi_stock_item_id = p.StockItemID
       AND p.lookup_time > s.valid_from AND p.lookup_time <= s.valid_to
)
SELECT CAST(ROW_NUMBER() OVER (ORDER BY p.PurchaseOrderID, p.PurchaseOrderLineID) AS BIGINT) AS purchase_key,
       CAST(p.OrderDate AS DATE) AS date_key,
       CAST(COALESCE(s.supplier_key, 0) AS INT) AS supplier_key,
       CAST(COALESCE(si.stock_item_key, 0) AS INT) AS stock_item_key,
       CAST(p.PurchaseOrderID AS INT) AS wwi_purchase_order_id,
       CAST(p.OrderedOuters AS INT) AS ordered_outers,
       CAST(p.ordered_quantity AS INT) AS ordered_quantity,
       CAST(p.ReceivedOuters AS INT) AS received_outers,
       CAST(p.PackageTypeName AS STRING) AS package,
       CAST(p.IsOrderLineFinalized AS BOOLEAN) AS is_order_finalized,
       CAST(10 AS INT) AS lineage_key
FROM staged p
LEFT JOIN supplier_matches s ON s.PurchaseOrderLineID = p.PurchaseOrderLineID AND s.match_rank = 1
LEFT JOIN stock_item_matches si ON si.PurchaseOrderLineID = p.PurchaseOrderLineID AND si.match_rank = 1;
