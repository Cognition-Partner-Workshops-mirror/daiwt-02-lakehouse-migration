USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:silver_schema);

CREATE OR REPLACE TEMP VIEW employee_expected AS
SELECT wwi_employee_id, employee, preferred_name, is_salesperson, photo, valid_from, valid_to FROM read_files(:golden_path || '/outputs/D0/tables/Dimension.Employee.csv',
  format => 'csv', header => true, schema => 'employee_key STRING, wwi_employee_id STRING, employee STRING, preferred_name STRING, is_salesperson STRING, photo STRING, valid_from STRING, valid_to STRING, lineage_key STRING',
  nullValue => '\\N', emptyValue => '', multiLine => true, escape => '"', mode => 'FAILFAST') c
WHERE false;

CREATE OR REPLACE TEMP VIEW employee_actual AS
SELECT cast(wwi_employee_id AS STRING) AS wwi_employee_id, cast(employee AS STRING) AS employee, cast(preferred_name AS STRING) AS preferred_name, cast(is_salesperson AS STRING) AS is_salesperson, cast(photo AS STRING) AS photo, cast(valid_from AS STRING) AS valid_from, cast(valid_to AS STRING) AS valid_to
FROM employee_changes;

SELECT CASE WHEN (SELECT count(*) FROM employee_actual) = (SELECT count(*) FROM employee_expected)
  THEN 'ok' ELSE raise_error('Employee D0 count mismatch') END;
SELECT CASE WHEN NOT EXISTS (SELECT * FROM employee_expected EXCEPT ALL SELECT * FROM employee_actual)
  THEN 'ok' ELSE raise_error('Employee D0 missing rows') END;
SELECT CASE WHEN NOT EXISTS (SELECT * FROM employee_actual EXCEPT ALL SELECT * FROM employee_expected)
  THEN 'ok' ELSE raise_error('Employee D0 unexpected rows') END;
SELECT CASE WHEN count(*) = 0 THEN 'ok' ELSE raise_error('Employee D0 quarantine is nonempty') END
FROM employee_changes_quarantine;
