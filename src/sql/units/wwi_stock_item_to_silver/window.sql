CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.stock_item_window')
AS SELECT :drop AS drop,
 CASE WHEN :drop = 'D0' THEN '1900-01-01 00:00:00.0000000' ELSE :last_cutoff END AS last_cutoff,
 :new_cutoff AS new_cutoff;
