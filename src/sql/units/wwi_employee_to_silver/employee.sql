CREATE OR REFRESH MATERIALIZED VIEW employee_changes (
  CONSTRAINT employee_id_present EXPECT (wwi_employee_id IS NOT NULL),
  CONSTRAINT valid_from_present EXPECT (valid_from IS NOT NULL),
  CONSTRAINT salesperson_bit EXPECT (is_salesperson IN (0, 1)),
  CONSTRAINT photo_hex EXPECT (photo IS NULL OR photo RLIKE '^([0-9A-F]{2})*$'),
  CONSTRAINT valid_interval EXPECT (valid_from < valid_to)
) AS
WITH fully_applied_drops AS (
  SELECT drop, drop_index
  FROM ${bronze_catalog}.${bronze_schema}.bronze_replay_log
  WHERE status = 'PASS'
  GROUP BY drop, drop_index
  HAVING count(*) = 38 AND count_if(status = 'PASS') = 38
    AND count(DISTINCT source_table) = 38
), employee_window AS (
  SELECT
    CASE WHEN instr(w.last_cutoff, '.') = 0 THEN concat(w.last_cutoff, '.0000000') ELSE w.last_cutoff END AS last_cutoff,
    CASE WHEN instr(w.new_cutoff, '.') = 0 THEN concat(w.new_cutoff, '.0000000') ELSE w.new_cutoff END AS new_cutoff
  FROM ${bronze_catalog}.${bronze_schema}.golden_drops w
  JOIN fully_applied_drops d ON w.drop = d.drop AND w.drop_index = d.drop_index
  WHERE w.table_name = 'Employee'
    AND d.drop_index = (SELECT max(drop_index) FROM fully_applied_drops)
), person_history AS (
  SELECT PersonID, FullName, PreferredName, IsEmployee, IsSalesperson, Photo, ValidFrom_str, ValidTo_str
  FROM ${bronze_catalog}.${bronze_schema}.application_people
  UNION ALL
  SELECT PersonID, FullName, PreferredName, IsEmployee, IsSalesperson, Photo, ValidFrom_str, ValidTo_str
  FROM ${bronze_catalog}.${bronze_schema}.application_people_archive
), events AS (
  SELECT p.PersonID, p.ValidFrom_str
  FROM person_history p CROSS JOIN employee_window w
  WHERE p.ValidFrom_str > w.last_cutoff AND p.ValidFrom_str <= w.new_cutoff
    AND p.IsEmployee = true
), person_as_of AS (
  SELECT p.* FROM events e JOIN person_history p
    ON p.PersonID = e.PersonID AND p.ValidFrom_str <= e.ValidFrom_str AND p.ValidTo_str > e.ValidFrom_str
), staged AS (
  SELECT PersonID AS wwi_employee_id, FullName AS employee,
    PreferredName AS preferred_name, cast(IsSalesperson AS INT) AS is_salesperson,
    upper(Photo) AS photo, ValidFrom_str AS valid_from
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
