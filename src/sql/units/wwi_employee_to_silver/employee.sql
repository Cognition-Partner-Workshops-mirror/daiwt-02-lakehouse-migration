CREATE OR REFRESH MATERIALIZED VIEW employee_changes (
  CONSTRAINT employee_id_present EXPECT (wwi_employee_id IS NOT NULL),
  CONSTRAINT valid_from_present EXPECT (valid_from IS NOT NULL),
  CONSTRAINT salesperson_bit EXPECT (is_salesperson IN (0, 1)),
  CONSTRAINT photo_hex EXPECT (photo IS NULL OR photo RLIKE '^([0-9A-F]{2})*$'),
  CONSTRAINT valid_interval EXPECT (valid_from < valid_to)
) AS
WITH person_history AS (
  SELECT * EXCEPT (_loaded_drop) FROM ${bronze_catalog}.${bronze_schema}.application_people
  UNION ALL
  SELECT * EXCEPT (_loaded_drop) FROM ${bronze_catalog}.${bronze_schema}.application_people_archive
), events AS (
  SELECT p.PersonID, p.ValidFrom
  FROM person_history p CROSS JOIN ${bronze_catalog}.${bronze_schema}.employee_drop_window w
  WHERE p.ValidFrom > w.last_cutoff AND p.ValidFrom <= w.new_cutoff
    AND try_cast(p.IsEmployee AS INT) <> 0
), person_as_of AS (
  SELECT p.* FROM events e JOIN person_history p
    ON p.PersonID = e.PersonID AND p.ValidFrom <= e.ValidFrom AND p.ValidTo > e.ValidFrom
), staged AS (
  SELECT try_cast(PersonID AS INT) AS wwi_employee_id, FullName AS employee,
    PreferredName AS preferred_name, try_cast(IsSalesperson AS INT) AS is_salesperson,
    upper(Photo) AS photo, ValidFrom AS valid_from
  FROM person_as_of
)
SELECT s.wwi_employee_id, s.employee, s.preferred_name, s.is_salesperson, s.photo, s.valid_from,
  coalesce((SELECT min(n.valid_from) FROM staged n
    WHERE n.wwi_employee_id = s.wwi_employee_id AND n.valid_from > s.valid_from),
    '9999-12-31 23:59:59.9999999') AS valid_to
FROM staged s;

CREATE OR REFRESH MATERIALIZED VIEW employee_changes_quarantine AS
SELECT *, concat_ws(';',
  CASE WHEN wwi_employee_id IS NULL THEN 'null id' END,
  CASE WHEN valid_from IS NULL THEN 'null valid_from' END,
  CASE WHEN is_salesperson IS NULL OR is_salesperson NOT IN (0, 1) THEN 'invalid salesperson' END,
  CASE WHEN photo IS NOT NULL AND NOT (photo RLIKE '^([0-9A-F]{2})*$') THEN 'invalid photo' END,
  CASE WHEN valid_from IS NULL OR valid_to IS NULL OR NOT (valid_from < valid_to) THEN 'invalid interval' END
) AS reason
FROM employee_changes
WHERE wwi_employee_id IS NULL OR valid_from IS NULL
  OR is_salesperson IS NULL OR is_salesperson NOT IN (0, 1)
  OR (photo IS NOT NULL AND NOT (photo RLIKE '^([0-9A-F]{2})*$'))
  OR valid_to IS NULL OR NOT (valid_from < valid_to);
