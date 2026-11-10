-- S09 Order: record this run's extract window (last_cutoff, new_cutoff] for the pipeline.
-- D0 is the restored baseline load, staged from the reseed cutoff. Bronze must have the
-- requested drop as its last fully applied replay.

SELECT CASE
  WHEN NOT (:drop RLIKE '^D[0-9]+$') THEN raise_error('invalid drop: ' || :drop)
  WHEN NOT (:last_cutoff RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}$')
    OR NOT (:new_cutoff RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}$')
    OR NOT (:baseline_cutoff RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}$')
    THEN raise_error('cutoffs must be yyyy-MM-dd HH:mm:ss.fffffff')
  ELSE 'ok' END AS window_check;

SELECT CASE
  WHEN max(drop_index) = CAST(substr(:drop, 2) AS INT)
   AND count_if(drop = :drop AND status = 'PASS') > 0
   AND count_if(drop = :drop AND status NOT IN ('PASS', 'STARTED')) = 0 THEN 'ok'
  ELSE raise_error('bronze replay of ' || :drop || ' is not the last fully applied drop') END AS bronze_check
FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.bronze_replay_log');

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :silver_schema || '.order_extract_window')
COMMENT 'Order extract window (last_cutoff, new_cutoff] of the current run.' AS
SELECT :drop AS drop,
  CASE WHEN :drop = 'D0' THEN :baseline_cutoff ELSE :last_cutoff END AS last_cutoff,
  :new_cutoff AS new_cutoff;
