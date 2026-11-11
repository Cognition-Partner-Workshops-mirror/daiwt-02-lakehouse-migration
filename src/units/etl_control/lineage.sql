CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.integration_lineage') AS
SELECT
    CAST(lineage_key AS INT) AS lineage_key,
    CAST(data_load_started AS TIMESTAMP_NTZ) AS data_load_started,
    CAST(table_name AS STRING) AS table_name,
    CAST(data_load_completed AS TIMESTAMP_NTZ) AS data_load_completed,
    CAST(1 AS BOOLEAN) AS was_successful,
    CAST('2016-06-06 12:31:17.0000000' AS TIMESTAMP_NTZ) AS source_system_cutoff_time
FROM VALUES
    (1, '2016-06-06 12:36:16.5814702', 'City', '2016-06-06 12:36:49.0592140'),
    (2, '2016-06-06 12:36:49.0748362', 'Customer', '2016-06-06 12:36:49.7154516'),
    (3, '2016-06-06 12:36:49.7310770', 'Employee', '2016-06-06 12:36:49.9654784'),
    (4, '2016-06-06 12:36:49.9810784', 'Payment Method', '2016-06-06 12:36:50.0748072'),
    (5, '2016-06-06 12:36:50.0748072', 'Stock Item', '2016-06-06 12:36:50.4342030'),
    (6, '2016-06-06 12:36:50.4498392', 'Supplier', '2016-06-06 12:36:50.5904519'),
    (7, '2016-06-06 12:36:50.5904519', 'Transaction Type', '2016-06-06 12:36:50.6842030'),
    (8, '2016-06-06 12:36:50.6998369', 'Movement', '2016-06-06 12:37:00.9810989'),
    (9, '2016-06-06 12:37:00.9967252', 'Order', '2016-06-06 12:37:12.4655543'),
    (10, '2016-06-06 12:37:12.4811498', 'Purchase', '2016-06-06 12:37:13.2624167'),
    (11, '2016-06-06 12:37:13.2624167', 'Sale', '2016-06-06 12:37:24.6856386'),
    (12, '2016-06-06 12:37:24.7012613', 'Stock Holding', '2016-06-06 12:37:24.8106071'),
    (13, '2016-06-06 12:37:24.8106071', 'Transaction', '2016-06-06 12:37:28.4200431')
AS baseline(lineage_key, data_load_started, table_name, data_load_completed);
