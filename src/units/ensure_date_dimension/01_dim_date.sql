CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_date') AS
WITH dates AS (
  SELECT explode(sequence(DATE '2013-01-01', DATE '2016-12-31', INTERVAL 1 DAY)) AS date
), attributes AS (
  SELECT
    date,
    dayofmonth(date) AS day_number,
    date_format(date, 'MMMM') AS month,
    date_format(date, 'MMM') AS short_month,
    month(date) AS calendar_month_number,
    year(date) AS calendar_year,
    (month(date) + 1) % 12 + 1 AS fiscal_month_number,
    year(date) + CASE WHEN month(date) >= 11 THEN 1 ELSE 0 END AS fiscal_year
  FROM dates
)
SELECT
  CAST(date AS DATE) AS date,
  CAST(day_number AS INT) AS day_number,
  CAST(day_number AS STRING) AS day,
  CAST(month AS STRING) AS month,
  CAST(short_month AS STRING) AS short_month,
  CAST(calendar_month_number AS INT) AS calendar_month_number,
  CAST(concat('CY', calendar_year, '-', short_month) AS STRING) AS calendar_month_label,
  CAST(calendar_year AS INT) AS calendar_year,
  CAST(concat('CY', calendar_year) AS STRING) AS calendar_year_label,
  CAST(fiscal_month_number AS INT) AS fiscal_month_number,
  CAST(concat('FY', fiscal_year, '-', short_month) AS STRING) AS fiscal_month_label,
  CAST(fiscal_year AS INT) AS fiscal_year,
  CAST(concat('FY', fiscal_year) AS STRING) AS fiscal_year_label,
  CAST(weekofyear(date) AS INT) AS iso_week_number
FROM attributes;
