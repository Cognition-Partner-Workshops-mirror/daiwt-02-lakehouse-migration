-- S09 Order parity for the current drop: the staged change set (silver order_changes) must equal
-- the rows gold Fact.Order gains from D(n-1) to D(n) (D0: all baseline rows), projected to staged
-- columns. Dimension keys are compared as (WWI ID, Valid From) of the golden dimension row,
-- resolving staged IDs with the point-in-time rule Valid From < Last Modified When <= Valid To.
-- The quarantine must be empty. Results accumulate per drop in order_changes_parity; any failure raises.

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.' || :silver_schema || '.order_changes_parity') (
  drop STRING, staged_rows BIGINT, golden_gained_rows BIGINT, missing_rows BIGINT, extra_rows BIGINT,
  quarantine_rows BIGINT, status STRING, checked_at TIMESTAMP)
COMMENT 'S09 Order per-drop parity of order_changes against golden Fact.Order gains.';

DELETE FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.order_changes_parity') WHERE :drop = 'D0' OR drop = :drop;

INSERT INTO IDENTIFIER(:catalog || '.' || :silver_schema || '.order_changes_parity')
WITH golden_fact AS (
  SELECT CAST(regexp_extract(_metadata.file_path, '/outputs/D([0-9]+)/', 1) AS INT) AS drop_seq, *
  FROM read_files('/Volumes/daiwt_02_mig_prod/landing/golden/outputs/D*/tables/Fact.Order.csv',
    format => 'csv', header => true, inferColumnTypes => false, nullValue => '\\N', escape => '"', multiLine => true, mode => 'FAILFAST', schemaEvolutionMode => 'none')
),
fact_counts AS (
  SELECT drop_seq, to_json(struct(* EXCEPT (drop_seq))) AS row_text, count(*) AS n FROM golden_fact GROUP BY ALL
),
fact_gained AS (
  SELECT cur.drop_seq, cur.row_text, cur.n - coalesce(prev.n, 0) AS n
  FROM fact_counts cur LEFT JOIN fact_counts prev ON prev.drop_seq = cur.drop_seq - 1 AND prev.row_text = cur.row_text
  WHERE cur.drop_seq = CAST(substr(:drop, 2) AS INT) AND cur.n - coalesce(prev.n, 0) > 0
),
dims AS (
  SELECT CAST(regexp_extract(_metadata.file_path, '/outputs/D([0-9]+)/', 1) AS INT) AS drop_seq, 'City' AS dim,
    city_key AS dim_key, wwi_city_id AS wwi_id, valid_from, valid_to
  FROM read_files('/Volumes/daiwt_02_mig_prod/landing/golden/outputs/D*/tables/Dimension.City.csv',
    format => 'csv', header => true, inferColumnTypes => false, nullValue => '\\N', escape => '"', multiLine => true, mode => 'FAILFAST', schemaEvolutionMode => 'none')
  UNION ALL
  SELECT CAST(regexp_extract(_metadata.file_path, '/outputs/D([0-9]+)/', 1) AS INT) AS drop_seq, 'Customer' AS dim,
    customer_key AS dim_key, wwi_customer_id AS wwi_id, valid_from, valid_to
  FROM read_files('/Volumes/daiwt_02_mig_prod/landing/golden/outputs/D*/tables/Dimension.Customer.csv',
    format => 'csv', header => true, inferColumnTypes => false, nullValue => '\\N', escape => '"', multiLine => true, mode => 'FAILFAST', schemaEvolutionMode => 'none')
  UNION ALL
  SELECT CAST(regexp_extract(_metadata.file_path, '/outputs/D([0-9]+)/', 1) AS INT) AS drop_seq, 'Employee' AS dim,
    employee_key AS dim_key, wwi_employee_id AS wwi_id, valid_from, valid_to
  FROM read_files('/Volumes/daiwt_02_mig_prod/landing/golden/outputs/D*/tables/Dimension.Employee.csv',
    format => 'csv', header => true, inferColumnTypes => false, nullValue => '\\N', escape => '"', multiLine => true, mode => 'FAILFAST', schemaEvolutionMode => 'none')
  UNION ALL
  SELECT CAST(regexp_extract(_metadata.file_path, '/outputs/D([0-9]+)/', 1) AS INT) AS drop_seq, 'Stock_Item' AS dim,
    stock_item_key AS dim_key, wwi_stock_item_id AS wwi_id, valid_from, valid_to
  FROM read_files('/Volumes/daiwt_02_mig_prod/landing/golden/outputs/D*/tables/Dimension.Stock_Item.csv',
    format => 'csv', header => true, inferColumnTypes => false, nullValue => '\\N', escape => '"', multiLine => true, mode => 'FAILFAST', schemaEvolutionMode => 'none')
),
gained_rows AS (
  SELECT g.drop_seq, f.*, g.n
  FROM fact_gained g
  LATERAL VIEW json_tuple(g.row_text, 'city_key', 'customer_key', 'stock_item_key', 'salesperson_key', 'picker_key',
    'order_date_key', 'picked_date_key', 'wwi_order_id', 'wwi_backorder_id', 'description', 'package', 'quantity',
    'unit_price', 'tax_rate', 'total_excluding_tax', 'tax_amount', 'total_including_tax') f
    AS city_key, customer_key, stock_item_key, salesperson_key, picker_key, order_date_key, picked_date_key,
       wwi_order_id, wwi_backorder_id, description, package, quantity, unit_price, tax_rate,
       total_excluding_tax, tax_amount, total_including_tax
),
golden_projected AS (
  SELECT r.drop_seq, r.n, to_json(named_struct(
      'order_date_key', r.order_date_key, 'picked_date_key', r.picked_date_key, 'wwi_order_id', r.wwi_order_id,
      'wwi_backorder_id', r.wwi_backorder_id, 'description', r.description, 'package', r.package, 'quantity', r.quantity,
      'unit_price', r.unit_price, 'tax_rate', r.tax_rate, 'total_excluding_tax', r.total_excluding_tax,
      'tax_amount', r.tax_amount, 'total_including_tax', r.total_including_tax,
      'city', CASE WHEN r.city_key = '0' THEN '0' ELSE coalesce(dc.wwi_id || '@' || dc.valid_from, 'unresolved') END,
      'customer', CASE WHEN r.customer_key = '0' THEN '0' ELSE coalesce(du.wwi_id || '@' || du.valid_from, 'unresolved') END,
      'stock_item', CASE WHEN r.stock_item_key = '0' THEN '0' ELSE coalesce(ds.wwi_id || '@' || ds.valid_from, 'unresolved') END,
      'salesperson', CASE WHEN r.salesperson_key = '0' THEN '0' ELSE coalesce(de.wwi_id || '@' || de.valid_from, 'unresolved') END,
      'picker', CASE WHEN r.picker_key = '0' THEN '0' ELSE coalesce(dp.wwi_id || '@' || dp.valid_from, 'unresolved') END
    )) AS row_text
  FROM gained_rows r
  LEFT JOIN dims dc ON dc.drop_seq = r.drop_seq AND dc.dim = 'City' AND dc.dim_key = r.city_key
  LEFT JOIN dims du ON du.drop_seq = r.drop_seq AND du.dim = 'Customer' AND du.dim_key = r.customer_key
  LEFT JOIN dims ds ON ds.drop_seq = r.drop_seq AND ds.dim = 'Stock_Item' AND ds.dim_key = r.stock_item_key
  LEFT JOIN dims de ON de.drop_seq = r.drop_seq AND de.dim = 'Employee' AND de.dim_key = r.salesperson_key
  LEFT JOIN dims dp ON dp.drop_seq = r.drop_seq AND dp.dim = 'Employee' AND dp.dim_key = r.picker_key
),
staged AS (
  SELECT CAST(substr(_drop, 2) AS INT) AS drop_seq, *
  FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.order_changes')
),
staged_lookups AS (
  SELECT s.drop_seq, s._order_line_id, d.dim, d.role, min(d.wwi_id || '@' || d.valid_from) AS token
  FROM staged s
  JOIN (SELECT *, 'city' AS role FROM dims WHERE dim = 'City'
        UNION ALL SELECT *, 'customer' FROM dims WHERE dim = 'Customer'
        UNION ALL SELECT *, 'stock_item' FROM dims WHERE dim = 'Stock_Item'
        UNION ALL SELECT *, 'salesperson' FROM dims WHERE dim = 'Employee'
        UNION ALL SELECT *, 'picker' FROM dims WHERE dim = 'Employee') d
    ON d.drop_seq = s.drop_seq
   AND d.wwi_id = CAST(CASE d.role WHEN 'city' THEN s.wwi_city_id WHEN 'customer' THEN s.wwi_customer_id
                    WHEN 'stock_item' THEN s.wwi_stock_item_id WHEN 'salesperson' THEN s.wwi_salesperson_id
                    ELSE s.wwi_picker_id END AS STRING)
   AND s._last_modified_when_text > d.valid_from AND s._last_modified_when_text <= d.valid_to
  GROUP BY ALL
),
staged_projected AS (
  SELECT s.drop_seq, to_json(named_struct(
      'order_date_key', CAST(s.order_date_key AS STRING), 'picked_date_key', CAST(s.picked_date_key AS STRING),
      'wwi_order_id', CAST(s.wwi_order_id AS STRING), 'wwi_backorder_id', CAST(s.wwi_backorder_id AS STRING),
      'description', s.description, 'package', s.package, 'quantity', CAST(s.quantity AS STRING),
      'unit_price', CAST(s.unit_price AS STRING), 'tax_rate', CAST(s.tax_rate AS STRING),
      'total_excluding_tax', CAST(s.total_excluding_tax AS STRING), 'tax_amount', CAST(s.tax_amount AS STRING),
      'total_including_tax', CAST(s.total_including_tax AS STRING),
      'city', coalesce(max(CASE WHEN l.role = 'city' THEN l.token END), '0'),
      'customer', coalesce(max(CASE WHEN l.role = 'customer' THEN l.token END), '0'),
      'stock_item', coalesce(max(CASE WHEN l.role = 'stock_item' THEN l.token END), '0'),
      'salesperson', coalesce(max(CASE WHEN l.role = 'salesperson' THEN l.token END), '0'),
      'picker', coalesce(max(CASE WHEN l.role = 'picker' THEN l.token END), '0')
    )) AS row_text
  FROM staged s LEFT JOIN staged_lookups l ON l.drop_seq = s.drop_seq AND l._order_line_id = s._order_line_id
  GROUP BY s.drop_seq, s._order_line_id, s.order_date_key, s.picked_date_key, s.wwi_order_id, s.wwi_backorder_id,
    s.description, s.package, s.quantity, s.unit_price, s.tax_rate, s.total_excluding_tax, s.tax_amount, s.total_including_tax
),
golden_multiset AS (SELECT drop_seq, row_text, sum(n) AS n FROM golden_projected GROUP BY ALL),
staged_multiset AS (SELECT drop_seq, row_text, count(*) AS n FROM staged_projected GROUP BY ALL),
diff AS (
  SELECT coalesce(g.drop_seq, s.drop_seq) AS drop_seq,
    greatest(coalesce(g.n, 0) - coalesce(s.n, 0), 0) AS missing,
    greatest(coalesce(s.n, 0) - coalesce(g.n, 0), 0) AS extra
  FROM golden_multiset g FULL OUTER JOIN staged_multiset s ON g.drop_seq = s.drop_seq AND g.row_text = s.row_text
),
drops AS (SELECT CAST(substr(:drop, 2) AS INT) AS drop_seq),
quarantine AS (
  SELECT CAST(substr(_drop, 2) AS INT) AS drop_seq, count(*) AS n
  FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.order_changes_quarantine') GROUP BY ALL
),
summary AS (
  SELECT 'D' || d.drop_seq AS drop,
    (SELECT count(*) FROM staged s WHERE s.drop_seq = d.drop_seq) AS staged_rows,
    (SELECT coalesce(sum(n), 0) FROM golden_multiset g WHERE g.drop_seq = d.drop_seq) AS golden_gained_rows,
    (SELECT coalesce(sum(missing), 0) FROM diff x WHERE x.drop_seq = d.drop_seq) AS missing_rows,
    (SELECT coalesce(sum(extra), 0) FROM diff x WHERE x.drop_seq = d.drop_seq) AS extra_rows,
    coalesce((SELECT max(n) FROM quarantine q WHERE q.drop_seq = d.drop_seq), 0) AS quarantine_rows
  FROM drops d
)
SELECT *, CASE WHEN staged_rows = golden_gained_rows AND staged_rows > 0 AND missing_rows = 0 AND extra_rows = 0
                    AND quarantine_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
  current_timestamp() AS checked_at
FROM summary;

SELECT CASE WHEN count(*) = 1 AND count_if(status = 'PASS') = 1 THEN 'order_changes parity PASS for ' || :drop
            ELSE raise_error('order_changes parity FAIL: ' || to_json(collect_list(struct(drop, staged_rows, golden_gained_rows, missing_rows, extra_rows, quarantine_rows, status)))) END AS result
FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.order_changes_parity')
WHERE drop = :drop;
