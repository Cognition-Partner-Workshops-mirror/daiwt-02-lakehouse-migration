# Silver temporal views

Run unit `silver_temporal_views` after `bronze_landing_replay` for each
drop D0 → D1 → D2. The job replaces views/functions idempotently and then
fails on known-row, uniqueness or interval-overlap violations. It does not
replay bronze itself. There are 14 temporal pairs in the checked-in spec
(older wave-0 notes say 13).
The Python task lives at
`src/sql/units/silver_temporal_views/temporal_views.py`, so the existing unit
directory classifier selects it without custom path mappings.

Each `silver.<bronze_table>_history` exposes every typed bronze column
(including exact timestamp string copies and ingestion metadata), plus
`valid_from`, `valid_to` STRING and `_version_source` (current/archive).
`silver.<bronze_table>_asof(t)` returns rows in the half-open interval
`valid_from <= t AND valid_to > t`. Always pass a fixed-width 27-character
`yyyy-MM-dd HH:mm:ss.fffffff` string: timestamps would lose the seventh digit.
Format validation checks the shape, not calendar validity.
These views reflect the latest replayed bronze drop, not independent drop snapshots.
Golden archives include zero-duration versions (People, StateProvinces and
Customers). History retains them, but AS OF never selects them; non-overlap
checks ignore these empty intervals and still reject reversed intervals.

`silver.temporal_change_events` contains distinct (source_table, valid_from).
`silver.change_events(tables, last_cutoff, new_cutoff)` returns distinct ordered
instants in (last_cutoff, new_cutoff], restricted to the supplied bronze table
names. An empty table array yields no events. Callers must apply the legacy
`valid_from <> @InitialLoadDate` exclusion themselves; it is intentionally
not baked into the shared helper. Apply a final ORDER BY when consuming events.

For City, include changes to all three driving tables, then reconstruct each
join at the event instant (replace catalog/schema with your bundle values):

```sql
SELECT e.valid_from, c.CityID, c.CityName, s.StateProvinceName, n.CountryName
FROM catalog.silver.change_events(
  array('application_cities', 'application_state_provinces', 'application_countries'),
  '2016-06-06 12:31:17.0000000', '2016-06-21 00:00:00.0000000') e,
LATERAL catalog.silver.application_cities_asof(e.valid_from) c,
LATERAL catalog.silver.application_state_provinces_asof(e.valid_from) s,
LATERAL catalog.silver.application_countries_asof(e.valid_from) n
WHERE c.StateProvinceID = s.StateProvinceID AND s.CountryID = n.CountryID
  AND e.valid_from <> '2013-01-01 00:00:00.0000000'
ORDER BY e.valid_from, c.CityID;
```

Cleanup integration: the current `ci/cleanup.py --prod` reads only `outputs`
and emits only DROP TABLE. P7 must consume `owns` and distinguish DROP VIEW
and DROP FUNCTION (all 15 views and 15 functions are registered here).
PR-schema cleanup already uses DROP SCHEMA CASCADE.
