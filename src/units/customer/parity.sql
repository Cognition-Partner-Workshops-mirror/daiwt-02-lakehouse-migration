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
WITH sales_customers AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.sales_customers')
 UNION ALL SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.sales_customers_archive')
), sales_customercategories AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.sales_customercategories')
 UNION ALL SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.sales_customercategories_archive')
), sales_buyinggroups AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.sales_buyinggroups')
 UNION ALL SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.sales_buyinggroups_archive')
), application_people AS (
 SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.application_people')
 UNION ALL SELECT * FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.application_people_archive')
), events AS (
 SELECT 'customer' kind, CustomerID id, ValidFrom t FROM sales_customers
 WHERE ValidFrom <= '2016-06-06 12:31:17.0000000'
 UNION ALL
 SELECT 'category', CustomerCategoryID, ValidFrom FROM sales_customercategories
 WHERE ValidFrom <= '2016-06-06 12:31:17.0000000' AND ValidFrom <> '2020-01-01 00:00:00.0000000'
 UNION ALL
 SELECT 'group', BuyingGroupID, ValidFrom FROM sales_buyinggroups
 WHERE ValidFrom <= '2016-06-06 12:31:17.0000000' AND ValidFrom <> '2020-01-01 00:00:00.0000000'
), changes AS (
 SELECT CAST(c.CustomerID AS INT) wwi_customer_id, c.CustomerName customer,
 bt.CustomerName bill_to_customer, cc.CustomerCategoryName category,
 bg.BuyingGroupName buying_group, p.FullName primary_contact,
 c.DeliveryPostalCode postal_code, c.ValidFrom valid_from
 FROM events e
 JOIN sales_customers c ON c.ValidFrom <= e.t AND e.t < c.ValidTo
 AND ((e.kind = 'customer' AND c.CustomerID = e.id)
   OR (e.kind = 'category' AND c.CustomerCategoryID = e.id)
   OR (e.kind = 'group' AND c.BuyingGroupID = e.id))
 JOIN sales_customercategories cc ON c.CustomerCategoryID = cc.CustomerCategoryID
 AND cc.ValidFrom <= e.t AND e.t < cc.ValidTo
 JOIN sales_customers bt ON c.BillToCustomerID = bt.CustomerID
 AND bt.ValidFrom <= e.t AND e.t < bt.ValidTo
 JOIN application_people p ON c.PrimaryContactPersonID = p.PersonID
 AND p.ValidFrom <= e.t AND e.t < p.ValidTo
 LEFT JOIN sales_buyinggroups bg ON c.BuyingGroupID = bg.BuyingGroupID
 AND bg.ValidFrom <= e.t AND e.t < bg.ValidTo
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
 ELSE raise_error('Customer full-history baseline mismatch; inspect baseline_actual EXCEPT ALL baseline_expected and reverse') END AS baseline_result;
