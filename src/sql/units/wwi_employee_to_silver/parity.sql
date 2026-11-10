USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:silver_schema);

SELECT CASE WHEN :drop IN ('D0', 'D1', 'D2') AND EXISTS (
  SELECT 1 FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.golden_drops') w
  WHERE w.table_name = 'Employee' AND w.drop = :drop
    AND rpad(w.last_cutoff, 27, CASE WHEN instr(w.last_cutoff, '.') = 0 THEN '.0000000' ELSE '0' END) = :last_cutoff
    AND rpad(w.new_cutoff, 27, CASE WHEN instr(w.new_cutoff, '.') = 0 THEN '.0000000' ELSE '0' END) = :new_cutoff
    AND w.drop_index = (
      SELECT max(drop_index) FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.bronze_replay_log')
    )
    AND 38 = (
      SELECT count(DISTINCT source_table)
      FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.bronze_replay_log')
      WHERE drop = :drop AND status = 'PASS'
    )
) THEN 'ok' ELSE raise_error('Employee replay window does not match requested drop/cutoffs') END;

CREATE OR REPLACE TEMP VIEW employee_expected AS
SELECT wwi_employee_id, employee, preferred_name, is_salesperson, photo, valid_from, valid_to FROM read_files(:golden_path || '/outputs/' || :drop || '/tables/Dimension.Employee.csv',
  format => 'csv', header => true, schema => 'employee_key STRING, wwi_employee_id STRING, employee STRING, preferred_name STRING, is_salesperson STRING, photo STRING, valid_from STRING, valid_to STRING, lineage_key STRING',
  nullValue => '\\N', emptyValue => '', multiLine => true, escape => '"', mode => 'FAILFAST') c
WHERE :drop <> 'D0' AND NOT EXISTS (SELECT 1 FROM read_files(:golden_path || '/outputs/' || CASE :drop WHEN 'D2' THEN 'D1' ELSE 'D0' END || '/tables/Dimension.Employee.csv',
  format => 'csv', header => true, schema => 'employee_key STRING, wwi_employee_id STRING, employee STRING, preferred_name STRING, is_salesperson STRING, photo STRING, valid_from STRING, valid_to STRING, lineage_key STRING',
  nullValue => '\\N', emptyValue => '', multiLine => true, escape => '"', mode => 'FAILFAST') p WHERE p.employee_key = c.employee_key);

CREATE OR REPLACE TEMP VIEW employee_actual AS
SELECT cast(wwi_employee_id AS STRING) AS wwi_employee_id, cast(employee AS STRING) AS employee, cast(preferred_name AS STRING) AS preferred_name, cast(is_salesperson AS STRING) AS is_salesperson, cast(photo AS STRING) AS photo, cast(valid_from AS STRING) AS valid_from, cast(valid_to AS STRING) AS valid_to
FROM employee_changes;

SELECT CASE WHEN (SELECT count(*) FROM employee_actual) = (SELECT count(*) FROM employee_expected)
  THEN 'ok' ELSE raise_error('Employee count mismatch') END;
SELECT CASE WHEN NOT EXISTS (SELECT * FROM employee_expected EXCEPT ALL SELECT * FROM employee_actual)
  THEN 'ok' ELSE raise_error('Employee missing rows') END;
SELECT CASE WHEN NOT EXISTS (SELECT * FROM employee_actual EXCEPT ALL SELECT * FROM employee_expected)
  THEN 'ok' ELSE raise_error('Employee unexpected rows') END;
SELECT CASE WHEN count(*) = 0 THEN 'ok' ELSE raise_error('Employee quarantine is nonempty') END
FROM employee_changes_quarantine;
