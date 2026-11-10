CREATE OR REFRESH MATERIALIZED VIEW transaction_type_changes (
  CONSTRAINT id_present EXPECT (wwi_transaction_type_id IS NOT NULL),
  CONSTRAINT name_present EXPECT (transaction_type IS NOT NULL AND length(transaction_type) > 0),
  CONSTRAINT valid_from_present EXPECT (valid_from IS NOT NULL),
  CONSTRAINT valid_interval EXPECT (valid_from < valid_to)
) AS
WITH versions AS (
  SELECT * FROM ${s07.catalog}.${s07.silver_schema}.s07_transaction_types_history
), events AS (
  SELECT v.TransactionTypeID, v.valid_from
  FROM versions v CROSS JOIN ${s07.catalog}.${s07.silver_schema}.s07_run_window w
  WHERE v.valid_from > w.last_cutoff AND v.valid_from <= w.new_cutoff
), staged AS (
  SELECT v.TransactionTypeID AS wwi_transaction_type_id,
         v.TransactionTypeName AS transaction_type, v.valid_from
  FROM events e JOIN versions v
    ON v.TransactionTypeID = e.TransactionTypeID
   AND v.valid_from <= e.valid_from AND e.valid_from < v.valid_to
)
SELECT s.wwi_transaction_type_id, s.transaction_type, s.valid_from,
       coalesce((SELECT min(n.valid_from) FROM staged n
                 WHERE n.wwi_transaction_type_id = s.wwi_transaction_type_id
                   AND n.valid_from > s.valid_from), '9999-12-31 23:59:59.9999999') AS valid_to
FROM staged s;

CREATE OR REFRESH MATERIALIZED VIEW transaction_type_changes_quarantine AS
SELECT * FROM transaction_type_changes
WHERE wwi_transaction_type_id IS NULL OR transaction_type IS NULL
   OR length(transaction_type) = 0 OR valid_from IS NULL OR valid_to IS NULL
   OR NOT (valid_from < valid_to);
