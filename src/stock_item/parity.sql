-- datetime2(7) remains a fixed-width string; Photo is decoded from SQL Server hex.
-- D0 dimension is the restored baseline (docs/wave0/00-units.md P5);
-- its tax_rate is not derived from D0 inputs.
WITH parity_policy AS (
 SELECT CASE WHEN :drop = 'D0' THEN array('tax_rate')
             ELSE CAST(array() AS ARRAY<STRING>) END AS excluded_columns
), current_dim AS (
 SELECT * FROM read_files(
  :golden_path || '/outputs/' || :drop || '/tables/Dimension.Stock_Item.csv',
  format => 'csv', header => true, inferSchema => false, nullValue => '\\N', escape => '"'
 )
), previous_dim AS (
 SELECT * FROM read_files(
  :golden_path || '/outputs/' || CASE WHEN :drop = 'D2' THEN 'D1' ELSE 'D0' END || '/tables/Dimension.Stock_Item.csv',
  format => 'csv', header => true, inferSchema => false, nullValue => '\\N', escape => '"'
 )
), expected AS (
 SELECT
  CAST(d.wwi_stock_item_id AS INT) AS wwi_stock_item_id,
  CAST(d.stock_item AS STRING) AS stock_item,
  CAST(d.color AS STRING) AS color,
  CAST(d.selling_package AS STRING) AS selling_package,
  CAST(d.buying_package AS STRING) AS buying_package,
  CAST(d.brand AS STRING) AS brand,
  CAST(d.size AS STRING) AS size,
  CAST(d.lead_time_days AS INT) AS lead_time_days,
  CAST(d.quantity_per_outer AS INT) AS quantity_per_outer,
  CAST(d.is_chiller_stock AS BOOLEAN) AS is_chiller_stock,
  CAST(d.barcode AS STRING) AS barcode,
  CAST(d.tax_rate AS DECIMAL(18,3)) AS tax_rate,
  CAST(d.unit_price AS DECIMAL(18,2)) AS unit_price,
  CAST(d.recommended_retail_price AS DECIMAL(18,2)) AS recommended_retail_price,
  CAST(d.typical_weight_per_unit AS DECIMAL(18,3)) AS typical_weight_per_unit,
  unhex(d.photo) AS photo,
  CAST(d.valid_from AS STRING) AS valid_from,
  CAST(d.valid_to AS STRING) AS valid_to
 FROM current_dim d WHERE d.stock_item_key <> '0'
 AND (:drop = 'D0' OR NOT EXISTS (
  SELECT 1 FROM previous_dim p WHERE p.stock_item_key = d.stock_item_key
 ))
), actual AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.stock_item_changes')
), expected_comparable AS (
 SELECT e.* EXCEPT (tax_rate),
 CASE WHEN array_contains(p.excluded_columns, 'tax_rate') THEN NULL ELSE e.tax_rate END AS tax_rate
 FROM expected e CROSS JOIN parity_policy p
), actual_comparable AS (
 SELECT a.* EXCEPT (tax_rate),
 CASE WHEN array_contains(p.excluded_columns, 'tax_rate') THEN NULL ELSE a.tax_rate END AS tax_rate
 FROM actual a CROSS JOIN parity_policy p
), missing AS (SELECT * FROM expected_comparable EXCEPT ALL SELECT * FROM actual_comparable),
extra AS (SELECT * FROM actual_comparable EXCEPT ALL SELECT * FROM expected_comparable)
SELECT CASE WHEN
 (SELECT count(*) FROM missing) = 0 AND (SELECT count(*) FROM extra) = 0
 AND (SELECT count(*) FROM actual) = (SELECT count(*) FROM expected)
 AND (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.stock_item_changes_quarantine')) = 0
 THEN 'PASS: Stock Item strict multiset parity and empty quarantine'
 ELSE raise_error('S05 Stock Item parity/quarantine mismatch') END AS parity,
 :drop AS drop_name,
 (SELECT count(*) FROM actual) AS actual_count,
 (SELECT count(*) FROM expected) AS expected_count,
 (SELECT count(*) FROM missing) AS missing_count,
 (SELECT count(*) FROM extra) AS extra_count,
 (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.stock_item_changes_quarantine')) AS quarantine_count;
