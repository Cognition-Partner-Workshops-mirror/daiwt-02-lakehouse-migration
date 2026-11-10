-- Lakeflow silver for the City unit: Integration.GetCityUpdates for every replayed drop window
-- (last_cutoff, new_cutoff]. Times are fixed-width datetime2(7) strings (silver *_history views),
-- compared lexically. AS OF t over current + archive history: ValidFrom <= t < ValidTo.

CREATE TEMPORARY VIEW city_hist AS
SELECT CityID, CityName, StateProvinceID, Location, LatestRecordedPopulation,
       valid_from AS ValidFrom, valid_to AS ValidTo
FROM ${silver}.application_cities_history;

CREATE TEMPORARY VIEW state_province_hist AS
SELECT StateProvinceID, StateProvinceName, CountryID, SalesTerritory,
       valid_from AS ValidFrom, valid_to AS ValidTo
FROM ${silver}.application_state_provinces_history;

CREATE TEMPORARY VIEW country_hist AS
SELECT CountryID, CountryName, Continent, Region, Subregion,
       valid_from AS ValidFrom, valid_to AS ValidTo
FROM ${silver}.application_countries_history;

-- City cutoff window of every drop the bronze replay has applied.
CREATE TEMPORARY VIEW city_extract_windows AS
SELECT d.`drop` AS _drop,
       concat(substr(d.last_cutoff, 1, 19), '.', rpad(substr(d.last_cutoff, 21, 7), 7, '0')) AS last_cutoff,
       concat(substr(d.new_cutoff, 1, 19), '.', rpad(substr(d.new_cutoff, 21, 7), 7, '0')) AS new_cutoff
FROM ${bronze}.golden_drops d
WHERE d.table_name = 'City'
  AND d.`drop` IN (SELECT `drop` FROM ${bronze}.bronze_replay_log WHERE status = 'PASS');

-- One change event per driving-table ValidFrom in the window (UNION ALL, no dedup), in cursor
-- order: Countries, then StateProvinces (both skip the legacy initial-load date), then Cities.
CREATE TEMPORARY VIEW city_change_events AS
SELECT w._drop, w.last_cutoff, w.new_cutoff, 1 AS driver_rank, 'Country' AS driver,
       co.CountryID AS driver_id, co.ValidFrom AS event_time
FROM country_hist co
JOIN city_extract_windows w
  ON co.ValidFrom > w.last_cutoff AND co.ValidFrom <= w.new_cutoff
WHERE co.ValidFrom <> '${initial_load_date}'
UNION ALL
SELECT w._drop, w.last_cutoff, w.new_cutoff, 2, 'StateProvince', sp.StateProvinceID, sp.ValidFrom
FROM state_province_hist sp
JOIN city_extract_windows w
  ON sp.ValidFrom > w.last_cutoff AND sp.ValidFrom <= w.new_cutoff
WHERE sp.ValidFrom <> '${initial_load_date}'
UNION ALL
SELECT w._drop, w.last_cutoff, w.new_cutoff, 3, 'City', c.CityID, c.ValidFrom
FROM city_hist c
JOIN city_extract_windows w
  ON c.ValidFrom > w.last_cutoff AND c.ValidFrom <= w.new_cutoff;

-- Each event re-reads the affected cities AS OF the event time. State/country are left-joined so
-- unresolved rows can be copied to quarantine; the extract itself keeps only resolved rows.
CREATE TEMPORARY VIEW city_event_rows AS
WITH affected AS (
  SELECT e.*, c.CityID, c.CityName, c.StateProvinceID, c.Location, c.LatestRecordedPopulation
  FROM city_change_events e
  JOIN city_hist c
    ON e.driver = 'City' AND c.CityID = e.driver_id
   AND c.ValidFrom <= e.event_time AND c.ValidTo > e.event_time
  UNION ALL
  SELECT e.*, c.CityID, c.CityName, c.StateProvinceID, c.Location, c.LatestRecordedPopulation
  FROM city_change_events e
  JOIN city_hist c
    ON e.driver = 'StateProvince' AND c.StateProvinceID = e.driver_id
   AND c.ValidFrom <= e.event_time AND c.ValidTo > e.event_time
  UNION ALL
  SELECT e.*, c.CityID, c.CityName, c.StateProvinceID, c.Location, c.LatestRecordedPopulation
  FROM city_change_events e
  JOIN state_province_hist s
    ON e.driver = 'Country' AND s.CountryID = e.driver_id
   AND s.ValidFrom <= e.event_time AND s.ValidTo > e.event_time
  JOIN city_hist c
    ON c.StateProvinceID = s.StateProvinceID
   AND c.ValidFrom <= e.event_time AND c.ValidTo > e.event_time
)
SELECT a._drop, a.last_cutoff, a.new_cutoff, a.driver_rank, a.driver, a.driver_id, a.event_time,
       a.CityID, a.CityName, sp.StateProvinceName, co.CountryName, co.Continent,
       sp.SalesTerritory, co.Region, co.Subregion, a.Location,
       COALESCE(a.LatestRecordedPopulation, 0) AS LatestRecordedPopulation,
       sp.StateProvinceID AS resolved_state_province_id, co.CountryID AS resolved_country_id
FROM affected a
LEFT JOIN state_province_hist sp
  ON sp.StateProvinceID = a.StateProvinceID
 AND sp.ValidFrom <= a.event_time AND sp.ValidTo > a.event_time
LEFT JOIN country_hist co
  ON co.CountryID = sp.CountryID
 AND co.ValidFrom <= a.event_time AND co.ValidTo > a.event_time;

-- Integration.City_Staging content per drop. Columns 1-12 follow the native result set order
-- (golden/native_result_sets.json City); Valid To = next later Valid From for the city, else end
-- of time.
CREATE OR REFRESH MATERIALIZED VIEW city_changes (
  CONSTRAINT wwi_city_id_present EXPECT (wwi_city_id IS NOT NULL),
  CONSTRAINT attributes_present EXPECT (city IS NOT NULL AND state_province IS NOT NULL
    AND country IS NOT NULL AND continent IS NOT NULL AND sales_territory IS NOT NULL
    AND region IS NOT NULL AND subregion IS NOT NULL),
  CONSTRAINT population_non_negative EXPECT (latest_recorded_population >= 0),
  CONSTRAINT location_is_point EXPECT (location IS NULL OR location LIKE 'POINT (%)'),
  CONSTRAINT valid_from_in_window EXPECT (valid_from > _last_cutoff AND valid_from <= _new_cutoff),
  CONSTRAINT validity_ordered EXPECT (valid_from < valid_to),
  CONSTRAINT datetime2_text EXPECT (valid_from RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}$'
    AND valid_to RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}$')
)
COMMENT 'S01 City: GetCityUpdates change set per drop window (Integration.City_Staging)'
AS
WITH staged AS (
  SELECT * FROM city_event_rows
  WHERE resolved_state_province_id IS NOT NULL AND resolved_country_id IS NOT NULL
),
next_version AS (
  SELECT _drop, CityID, event_time,
         LEAD(event_time) OVER (PARTITION BY _drop, CityID ORDER BY event_time) AS next_time
  FROM (SELECT DISTINCT _drop, CityID, event_time FROM staged)
)
SELECT s.CityID AS wwi_city_id, s.CityName AS city, s.StateProvinceName AS state_province,
       s.CountryName AS country, s.Continent AS continent, s.SalesTerritory AS sales_territory,
       s.Region AS region, s.Subregion AS subregion, s.Location AS location,
       CAST(s.LatestRecordedPopulation AS BIGINT) AS latest_recorded_population,
       s.event_time AS valid_from,
       COALESCE(n.next_time, '${end_of_time}') AS valid_to,
       s._drop, s.last_cutoff AS _last_cutoff, s.new_cutoff AS _new_cutoff,
       s.driver AS _change_driver, s.driver_id AS _change_driver_id,
       ROW_NUMBER() OVER (PARTITION BY s._drop
                          ORDER BY s.event_time, s.driver_rank, s.driver_id, s.CityID) AS _staged_order
FROM staged s
JOIN next_version n ON n._drop = s._drop AND n.CityID = s.CityID AND n.event_time = s.event_time;

-- Quarantine copy (never a drop): staged rows failing a city_changes expectation, plus event rows
-- the extract cannot resolve to a state/country AS OF the event. Empty for the golden inputs.
CREATE OR REFRESH MATERIALIZED VIEW city_changes_quarantine
COMMENT 'S01 City: quarantine copy of city change rows failing expectations or unresolved joins'
AS
SELECT _drop, 'unresolved_state_or_country' AS _reason, CityID AS wwi_city_id, CityName AS city,
       StateProvinceName AS state_province, CountryName AS country, Location AS location,
       event_time AS valid_from, CAST(NULL AS STRING) AS valid_to, driver AS _change_driver,
       driver_id AS _change_driver_id
FROM city_event_rows
WHERE resolved_state_province_id IS NULL OR resolved_country_id IS NULL
UNION ALL
SELECT _drop, CASE
         WHEN wwi_city_id IS NULL THEN 'wwi_city_id_present'
         WHEN city IS NULL OR state_province IS NULL OR country IS NULL OR continent IS NULL
           OR sales_territory IS NULL OR region IS NULL OR subregion IS NULL THEN 'attributes_present'
         WHEN COALESCE(latest_recorded_population, -1) < 0 THEN 'population_non_negative'
         WHEN NOT (location IS NULL OR location LIKE 'POINT (%)') THEN 'location_is_point'
         WHEN NOT (valid_from > _last_cutoff AND valid_from <= _new_cutoff) THEN 'valid_from_in_window'
         WHEN NOT valid_from < valid_to THEN 'validity_ordered'
         ELSE 'datetime2_text' END,
       wwi_city_id, city, state_province, country, location, valid_from, valid_to,
       _change_driver, _change_driver_id
FROM city_changes
WHERE (wwi_city_id IS NOT NULL
  AND city IS NOT NULL AND state_province IS NOT NULL AND country IS NOT NULL
  AND continent IS NOT NULL AND sales_territory IS NOT NULL AND region IS NOT NULL
  AND subregion IS NOT NULL
  AND latest_recorded_population >= 0
  AND (location IS NULL OR location LIKE 'POINT (%)')
  AND valid_from > _last_cutoff AND valid_from <= _new_cutoff
  AND valid_from < valid_to
  AND valid_from RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}$'
  AND valid_to RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}$') IS NOT TRUE;
