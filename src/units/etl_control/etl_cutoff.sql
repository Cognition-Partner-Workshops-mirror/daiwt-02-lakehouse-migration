CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.integration_etl_cutoff') AS
SELECT
    CAST(table_name AS STRING) AS table_name,
    CAST(CASE WHEN table_name = 'Date' THEN '2012-12-31 00:00:00.0000000'
              ELSE '2016-06-06 12:31:17.0000000' END AS TIMESTAMP_NTZ) AS cutoff_time
FROM VALUES
    ('City'),
    ('Customer'),
    ('Date'),
    ('Employee'),
    ('Movement'),
    ('Order'),
    ('Payment Method'),
    ('Purchase'),
    ('Sale'),
    ('Stock Holding'),
    ('Stock Item'),
    ('Supplier'),
    ('Transaction'),
    ('Transaction Type')
AS baseline(table_name);
