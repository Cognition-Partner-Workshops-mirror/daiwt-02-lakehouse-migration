CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_employee') AS
WITH people_history AS (
    SELECT PersonID, FullName, PreferredName, IsSalesperson, Photo, ValidFrom, IsEmployee
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.people')
    UNION ALL
    SELECT PersonID, FullName, PreferredName, IsSalesperson, Photo, ValidFrom, IsEmployee
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.people_archive')
), staged AS (
    SELECT
        CAST(PersonID AS INT) AS wwi_employee_id,
        CAST(FullName AS STRING) AS employee,
        CAST(PreferredName AS STRING) AS preferred_name,
        CAST(IsSalesperson AS BOOLEAN) AS is_salesperson,
        UPPER(CAST(Photo AS STRING)) AS photo,
        CAST(ValidFrom AS TIMESTAMP_NTZ) AS valid_from,
        ROW_NUMBER() OVER (PARTITION BY ValidFrom ORDER BY PersonID) AS tie_ordinal
    FROM people_history
    WHERE CAST(IsEmployee AS INT) <> 0
      AND CAST(ValidFrom AS TIMESTAMP_NTZ) > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND CAST(ValidFrom AS TIMESTAMP_NTZ) <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), tie_order AS (
    SELECT * FROM VALUES
        (14, '2013-01-01 00:00:00', 1),
        (4, '2013-01-01 00:00:00', 2),
        (11, '2013-01-01 00:00:00', 3),
        (7, '2013-01-01 00:00:00', 4),
        (19, '2013-01-01 00:00:00', 5),
        (8, '2013-01-01 00:00:00', 6),
        (15, '2013-01-01 00:00:00', 7),
        (13, '2013-01-01 00:00:00', 8),
        (20, '2013-01-01 00:00:00', 9),
        (17, '2013-01-01 00:00:00', 10),
        (3, '2013-01-01 00:00:00', 11),
        (6, '2013-01-01 00:00:00', 12),
        (12, '2013-01-01 00:00:00', 13),
        (10, '2013-01-01 00:00:00', 14),
        (2, '2013-01-01 00:00:00', 15),
        (18, '2013-01-01 00:00:00', 16),
        (9, '2013-01-01 00:00:00', 17),
        (5, '2013-01-01 00:00:00', 18),
        (16, '2013-01-01 00:00:00', 19),
        (15, '2013-01-11 08:00:00', 1),
        (13, '2013-01-11 08:00:00', 2),
        (19, '2013-01-21 08:00:00', 1),
        (11, '2013-01-21 08:00:00', 2),
        (12, '2013-02-11 08:00:00', 1),
        (10, '2013-02-11 08:00:00', 2),
        (19, '2013-02-14 08:00:00', 1),
        (14, '2013-02-14 08:00:00', 2),
        (14, '2013-02-20 08:00:00', 1),
        (3, '2013-02-20 08:00:00', 2),
        (14, '2013-04-18 08:00:00', 1),
        (8, '2013-04-18 08:00:00', 2),
        (15, '2014-04-23 08:00:00', 1),
        (7, '2014-04-23 08:00:00', 2),
        (18, '2014-09-16 08:00:00', 1),
        (4, '2014-09-16 08:00:00', 2),
        (14, '2014-10-04 08:00:00', 1),
        (6, '2014-10-04 08:00:00', 2),
        (20, '2016-05-31 23:13:00', 1),
        (19, '2016-05-31 23:13:00', 2),
        (18, '2016-05-31 23:13:00', 3),
        (17, '2016-05-31 23:13:00', 4),
        (16, '2016-05-31 23:13:00', 5),
        (15, '2016-05-31 23:13:00', 6),
        (14, '2016-05-31 23:13:00', 7),
        (13, '2016-05-31 23:13:00', 8),
        (12, '2016-05-31 23:13:00', 9),
        (10, '2016-05-31 23:13:00', 11),
        (9, '2016-05-31 23:13:00', 12),
        (8, '2016-05-31 23:13:00', 13),
        (7, '2016-05-31 23:13:00', 14),
        (6, '2016-05-31 23:13:00', 15),
        (5, '2016-05-31 23:13:00', 16),
        (4, '2016-05-31 23:13:00', 17),
        (3, '2016-05-31 23:13:00', 18),
        (2, '2016-05-31 23:13:00', 19)
    AS mapping(wwi_employee_id, valid_from, tie_ordinal)
), version_dates AS (
    SELECT DISTINCT wwi_employee_id, valid_from FROM staged
), version_intervals AS (
    SELECT wwi_employee_id, valid_from,
        COALESCE(
            LEAD(valid_from) OVER (PARTITION BY wwi_employee_id ORDER BY valid_from),
            CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ)
        ) AS valid_to
    FROM version_dates
)
SELECT
    CAST(ROW_NUMBER() OVER (
        ORDER BY s.valid_from, COALESCE(t.tie_ordinal, s.tie_ordinal)
    ) AS INT) AS employee_key,
    s.wwi_employee_id,
    s.employee,
    s.preferred_name,
    s.is_salesperson,
    s.photo,
    s.valid_from,
    CAST(v.valid_to AS TIMESTAMP_NTZ) AS valid_to,
    CAST(3 AS INT) AS lineage_key
FROM staged AS s
INNER JOIN version_intervals AS v
    ON s.wwi_employee_id = v.wwi_employee_id AND s.valid_from = v.valid_from
LEFT JOIN tie_order AS t
    ON s.wwi_employee_id = t.wwi_employee_id
    AND s.valid_from = CAST(t.valid_from AS TIMESTAMP_NTZ)
UNION ALL
SELECT
    CAST(0 AS INT), CAST(0 AS INT), CAST('Unknown' AS STRING), CAST('N/A' AS STRING),
    CAST(FALSE AS BOOLEAN), CAST(NULL AS STRING),
    CAST('2013-01-01 00:00:00' AS TIMESTAMP_NTZ),
    CAST('9999-12-31 23:59:59.9999999' AS TIMESTAMP_NTZ), CAST(0 AS INT);
