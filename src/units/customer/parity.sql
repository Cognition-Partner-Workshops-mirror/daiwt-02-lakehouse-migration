WITH golden_customer AS (
SELECT * FROM read_files(
  :golden_path || '/outputs/' || :drop || '/tables/Dimension.Customer.csv',
  format => 'csv', header => true, nullValue => '\\N',
  schema => 'customer_key INT, wwi_customer_id INT, customer STRING, bill_to_customer STRING, category STRING, buying_group STRING, primary_contact STRING, postal_code STRING, valid_from STRING, valid_to STRING, lineage_key INT')
),
previous_customer AS (
SELECT * FROM read_files(
  :golden_path || '/outputs/' || CASE :drop WHEN 'D2' THEN 'D1' ELSE 'D0' END || '/tables/Dimension.Customer.csv',
  format => 'csv', header => true, nullValue => '\\N',
  schema => 'customer_key INT, wwi_customer_id INT, customer STRING, bill_to_customer STRING, category STRING, buying_group STRING, primary_contact STRING, postal_code STRING, valid_from STRING, valid_to STRING, lineage_key INT')
),
seed_customer AS (
SELECT * FROM read_files(
  :golden_path || '/outputs/D0/tables/Dimension.Customer.csv',
  format => 'csv', header => true, nullValue => '\\N',
  schema => 'customer_key INT, wwi_customer_id INT, customer STRING, bill_to_customer STRING, category STRING, buying_group STRING, primary_contact STRING, postal_code STRING, valid_from STRING, valid_to STRING, lineage_key INT')
),
expected_changes AS (
SELECT wwi_customer_id, customer, bill_to_customer, category, buying_group,
       primary_contact, postal_code, valid_from, valid_to FROM golden_customer
EXCEPT ALL
SELECT wwi_customer_id, customer, bill_to_customer, category, buying_group,
       primary_contact, postal_code, valid_from, valid_to FROM previous_customer
),
actual_changes AS (
SELECT * FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.customer_changes')
),
baseline_actual AS (
-- Baseline reconstruction uses the initial load date 2013-01-01.
WITH sales_customers AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.sales_customers_history')
), sales_customercategories AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.sales_customer_categories_history')
), sales_buyinggroups AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.sales_buying_groups_history')
), application_people AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.application_people_history')
), events AS (
 SELECT 'customer' kind, CustomerID id, valid_from t FROM sales_customers
 WHERE valid_from <= '2016-06-06 12:31:17.0000000'
 UNION ALL
 SELECT 'category', CustomerCategoryID, valid_from FROM sales_customercategories
 WHERE valid_from <= '2016-06-06 12:31:17.0000000' AND valid_from <> '2013-01-01 00:00:00.0000000'
 UNION ALL
 SELECT 'group', BuyingGroupID, valid_from FROM sales_buyinggroups
 WHERE valid_from <= '2016-06-06 12:31:17.0000000' AND valid_from <> '2013-01-01 00:00:00.0000000'
), changes AS (
 SELECT CAST(c.CustomerID AS INT) wwi_customer_id, c.CustomerName customer,
 bt.CustomerName bill_to_customer, cc.CustomerCategoryName category,
 bg.BuyingGroupName buying_group, p.FullName primary_contact,
 c.DeliveryPostalCode postal_code, c.valid_from valid_from
 FROM events e
 JOIN LATERAL IDENTIFIER(:catalog || '.' || :silver_schema || '.sales_customers_asof')(e.t) c
 ON ((e.kind = 'customer' AND c.CustomerID = e.id)
   OR (e.kind = 'category' AND c.CustomerCategoryID = e.id)
   OR (e.kind = 'group' AND c.BuyingGroupID = e.id))
 JOIN LATERAL IDENTIFIER(:catalog || '.' || :silver_schema || '.sales_customer_categories_asof')(e.t) cc ON c.CustomerCategoryID = cc.CustomerCategoryID
 JOIN LATERAL IDENTIFIER(:catalog || '.' || :silver_schema || '.sales_customers_asof')(e.t) bt ON c.BillToCustomerID = bt.CustomerID
 JOIN LATERAL IDENTIFIER(:catalog || '.' || :silver_schema || '.application_people_asof')(e.t) p ON c.PrimaryContactPersonID = p.PersonID
 LEFT JOIN LATERAL IDENTIFIER(:catalog || '.' || :silver_schema || '.sales_buying_groups_asof')(e.t) bg ON c.BuyingGroupID = bg.BuyingGroupID
), times AS (
 SELECT wwi_customer_id, valid_from,
 LEAD(valid_from) OVER (PARTITION BY wwi_customer_id ORDER BY valid_from) next_from
 FROM (SELECT DISTINCT wwi_customer_id, valid_from FROM changes)
)
SELECT c.wwi_customer_id, c.customer, c.bill_to_customer, c.category, c.buying_group,
 c.primary_contact, c.postal_code, c.valid_from,
 COALESCE(t.next_from, '9999-12-31 23:59:59.9999999') valid_to
FROM changes c JOIN times t
 ON c.wwi_customer_id = t.wwi_customer_id AND c.valid_from = t.valid_from
WHERE c.buying_group IS NOT NULL
),
baseline_expected AS (
SELECT wwi_customer_id, customer, bill_to_customer, category, buying_group,
       primary_contact, postal_code, valid_from, valid_to
FROM seed_customer WHERE customer_key <> 0
)
SELECT CASE WHEN :drop IN ('D0', 'D1', 'D2')
 AND EXISTS (SELECT 1 FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.golden_drops')
 WHERE table_name = 'Customer' AND drop = :drop
 AND concat(split_part(last_cutoff, '.', 1), '.', rpad(split_part(last_cutoff, '.', 2), 7, '0')) = :last_cutoff
 AND concat(split_part(new_cutoff, '.', 1), '.', rpad(split_part(new_cutoff, '.', 2), 7, '0')) = :new_cutoff)
 AND :drop = (SELECT max_by(drop, drop_index) FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.bronze_replay_log') WHERE status = 'PASS')
 AND (SELECT count(*) FROM golden_customer) = 403
 AND (SELECT count(*) FROM golden_customer WHERE customer_key = 0) = 1
 AND (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.customer_changes_quarantine')) = 0
 AND (SELECT count(*) FROM actual_changes) = (SELECT count(*) FROM expected_changes)
 AND NOT EXISTS (SELECT * FROM actual_changes EXCEPT ALL SELECT * FROM expected_changes)
 AND NOT EXISTS (SELECT * FROM expected_changes EXCEPT ALL SELECT * FROM actual_changes)
 THEN 'PASS Customer staged change-set parity and quarantine'
 ELSE raise_error('Customer staged change-set/count/quarantine mismatch') END AS staged_result,
CASE WHEN (SELECT count(*) FROM seed_customer) = 403
 AND (SELECT count(*) FROM seed_customer WHERE customer_key = 0) = 1
 AND (SELECT count(*) FROM baseline_expected) = 402
 AND (SELECT count(*) FROM baseline_actual) = 402
 AND NOT EXISTS (SELECT * FROM baseline_actual EXCEPT ALL SELECT * FROM baseline_expected)
 AND NOT EXISTS (SELECT * FROM baseline_expected EXCEPT ALL SELECT * FROM baseline_actual)
 THEN 'PASS Customer full-history baseline: 402 exact rows'
 ELSE raise_error('Customer full-history baseline mismatch; inspect baseline_actual EXCEPT ALL baseline_expected and reverse') END AS baseline_result,
 (SELECT count(*) FROM actual_changes) AS staged_rows,
 (SELECT count(*) FROM expected_changes) AS golden_gain_rows,
 (SELECT count(*) FROM golden_customer) AS golden_rows,
 (SELECT count(*) FROM golden_customer WHERE customer_key = 0) AS golden_key_zero_rows,
 (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :silver_schema || '.customer_changes_quarantine')) AS quarantine_rows,
 (SELECT count(*) FROM baseline_actual) AS baseline_rows,
 (SELECT count(*) FROM (SELECT * FROM baseline_actual EXCEPT ALL SELECT * FROM baseline_expected)) AS baseline_extra,
 (SELECT count(*) FROM (SELECT * FROM baseline_expected EXCEPT ALL SELECT * FROM baseline_actual)) AS baseline_missing;
