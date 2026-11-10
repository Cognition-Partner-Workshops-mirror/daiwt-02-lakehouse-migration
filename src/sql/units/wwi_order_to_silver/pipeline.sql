-- S09 Order: silver staged change set reproducing Integration.GetOrderUpdates for the current
-- run's window (last_cutoff, new_cutoff], read from the bronze landing current-state tables.

-- Order lines in the window with every join resolved or left NULL (for quarantine).
-- Timestamps compare on their exact datetime2(7) text.
CREATE TEMPORARY VIEW order_lines_in_window AS
SELECT w.drop, w.last_cutoff, w.new_cutoff,
  l.OrderLineID, l.OrderID AS LineOrderID, o.OrderID, l.PackageTypeID, pt.PackageTypeID AS ResolvedPackageTypeID,
  o.CustomerID, c.CustomerID AS ResolvedCustomerID,
  o.OrderDate, l.PickingCompletedWhen, o.BackorderOrderID, l.Description, pt.PackageTypeName,
  l.Quantity, l.UnitPrice, l.TaxRate, c.DeliveryCityID, l.StockItemID,
  o.SalespersonPersonID, o.PickedByPersonID,
  CASE WHEN l.LastEditedWhen > o.LastEditedWhen THEN l.LastEditedWhen ELSE coalesce(o.LastEditedWhen, l.LastEditedWhen) END AS LastModifiedWhen
FROM ${window_fqn} w
CROSS JOIN (SELECT OrderLineID, OrderID, StockItemID, Description, PackageTypeID, Quantity, UnitPrice, TaxRate,
                   PickingCompletedWhen, LastEditedWhen_str AS LastEditedWhen
            FROM ${bronze_fqn}.sales_order_lines) l
LEFT JOIN (SELECT OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, BackorderOrderID, OrderDate,
                  LastEditedWhen_str AS LastEditedWhen
           FROM ${bronze_fqn}.sales_orders) o ON o.OrderID = l.OrderID
LEFT JOIN ${bronze_fqn}.warehouse_package_types pt ON pt.PackageTypeID = l.PackageTypeID
LEFT JOIN ${bronze_fqn}.sales_customers c ON c.CustomerID = o.CustomerID
WHERE (CASE WHEN l.LastEditedWhen > o.LastEditedWhen THEN l.LastEditedWhen ELSE coalesce(o.LastEditedWhen, l.LastEditedWhen) END) > w.last_cutoff
  AND (CASE WHEN l.LastEditedWhen > o.LastEditedWhen THEN l.LastEditedWhen ELSE coalesce(o.LastEditedWhen, l.LastEditedWhen) END) <= w.new_cutoff;

CREATE OR REFRESH MATERIALIZED VIEW order_changes (
  CONSTRAINT wwi_order_id_present EXPECT (wwi_order_id IS NOT NULL),
  CONSTRAINT wwi_stock_item_id_present EXPECT (wwi_stock_item_id IS NOT NULL),
  CONSTRAINT wwi_salesperson_id_present EXPECT (wwi_salesperson_id IS NOT NULL),
  CONSTRAINT order_date_present EXPECT (order_date_key IS NOT NULL),
  CONSTRAINT quantity_positive EXPECT (quantity > 0),
  CONSTRAINT unit_price_nonnegative EXPECT (unit_price >= 0),
  CONSTRAINT tax_rate_in_range EXPECT (tax_rate BETWEEN 0 AND 100),
  CONSTRAINT totals_consistent EXPECT (total_including_tax = total_excluding_tax + tax_amount),
  CONSTRAINT in_window EXPECT (_last_modified_when_text > _last_cutoff AND _last_modified_when_text <= _new_cutoff)
)
COMMENT 'Staged Order change set of the current drop: Integration.GetOrderUpdates result set (positional column order), plus _-prefixed lineage columns.'
TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')
AS SELECT
  CAST(OrderDate AS DATE) AS order_date_key,
  CAST(PickingCompletedWhen AS DATE) AS picked_date_key,
  CAST(OrderID AS INT) AS wwi_order_id,
  CAST(BackorderOrderID AS INT) AS wwi_backorder_id,
  Description AS description,
  PackageTypeName AS package,
  CAST(Quantity AS INT) AS quantity,
  CAST(UnitPrice AS DECIMAL(18,2)) AS unit_price,
  CAST(TaxRate AS DECIMAL(18,3)) AS tax_rate,
  CAST(CAST(Quantity AS DECIMAL(10,0)) * CAST(UnitPrice AS DECIMAL(18,2)) AS DECIMAL(18,2)) AS total_excluding_tax,
  CAST(round(CAST(Quantity AS DECIMAL(10,0)) * CAST(UnitPrice AS DECIMAL(18,2)) * CAST(TaxRate AS DECIMAL(18,3)), 0) / 100 AS DECIMAL(18,2)) AS tax_amount,
  CAST(CAST(Quantity AS DECIMAL(10,0)) * CAST(UnitPrice AS DECIMAL(18,2))
     + round(CAST(Quantity AS DECIMAL(10,0)) * CAST(UnitPrice AS DECIMAL(18,2)) * CAST(TaxRate AS DECIMAL(18,3)), 0) / 100 AS DECIMAL(18,2)) AS total_including_tax,
  CAST(DeliveryCityID AS INT) AS wwi_city_id,
  CAST(CustomerID AS INT) AS wwi_customer_id,
  CAST(StockItemID AS INT) AS wwi_stock_item_id,
  CAST(SalespersonPersonID AS INT) AS wwi_salesperson_id,
  CAST(PickedByPersonID AS INT) AS wwi_picker_id,
  CAST(substr(LastModifiedWhen, 1, 26) AS TIMESTAMP_NTZ) AS last_modified_when,
  drop AS _drop,
  last_cutoff AS _last_cutoff,
  new_cutoff AS _new_cutoff,
  LastModifiedWhen AS _last_modified_when_text,
  CAST(OrderLineID AS INT) AS _order_line_id,
  row_number() OVER (ORDER BY CAST(OrderID AS INT), CAST(OrderLineID AS INT)) AS _stage_seq
FROM order_lines_in_window
WHERE OrderID IS NOT NULL AND ResolvedPackageTypeID IS NOT NULL AND ResolvedCustomerID IS NOT NULL;

-- Quarantine copies (never removes rows from order_changes); empty for the golden inputs.
CREATE OR REFRESH MATERIALIZED VIEW order_changes_quarantine
COMMENT 'Copies of in-window Order source lines that fail a data-quality rule, with reasons.'
AS SELECT * FROM (
  SELECT drop AS _drop, CAST(OrderLineID AS INT) AS _order_line_id, LineOrderID AS order_id_text,
    LastModifiedWhen AS _last_modified_when_text,
    filter(array(
      CASE WHEN OrderID IS NULL THEN 'order_header_missing' END,
      CASE WHEN ResolvedPackageTypeID IS NULL THEN 'package_type_missing' END,
      CASE WHEN OrderID IS NOT NULL AND ResolvedCustomerID IS NULL THEN 'customer_missing' END,
      CASE WHEN Quantity IS NULL OR Quantity <= 0 THEN 'quantity_invalid' END,
      CASE WHEN UnitPrice IS NULL OR UnitPrice < 0 THEN 'unit_price_invalid' END,
      CASE WHEN TaxRate IS NULL OR TaxRate NOT BETWEEN 0 AND 100 THEN 'tax_rate_invalid' END,
      CASE WHEN LastModifiedWhen NOT RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}0$' THEN 'last_modified_when_precision' END,
      CASE WHEN OrderID IS NOT NULL AND SalespersonPersonID IS NULL THEN 'salesperson_missing' END
    ), r -> r IS NOT NULL) AS _reasons
  FROM order_lines_in_window
) WHERE size(_reasons) > 0;
