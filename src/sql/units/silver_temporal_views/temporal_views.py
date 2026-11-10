"""Exact datetime2(7) temporal history and half-open AS OF helpers."""
import argparse
import json
import re
from pathlib import Path


TIME_PATTERN = r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{7}"
SAMPLE_TIMES = ("2016-05-31 23:14:00.0000000", "2016-06-14 07:59:59.9999999",
                "2016-06-14 08:00:00.0000000")


def q(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name}")
    return f"`{name}`"


def temporal_tables(spec):
    names = {t["table"] for t in spec["tables"]}
    return [t for t in spec["tables"] if t["table"] + "_archive" in names]


def owned_objects(spec):
    return sorted([f"silver.{t['table']}_{suffix}" for t in temporal_tables(spec)
                   for suffix in ("history", "asof")] +
                  ["silver.temporal_change_events", "silver.change_events"])


def checked_time(expression):
    return (f"CASE WHEN {expression} IS NOT NULL AND {expression} RLIKE '^{TIME_PATTERN}$' "
            f"THEN {expression} ELSE CAST(raise_error('Expected yyyy-MM-dd HH:mm:ss.fffffff') AS STRING) END")


def statements(spec, catalog, bronze_schema, silver_schema):
    bronze = f"{q(catalog)}.{q(bronze_schema)}"
    silver = f"{q(catalog)}.{q(silver_schema)}"
    tables = temporal_tables(spec)
    for table in tables:
        name = table['table']
        history = f"{silver}.{q(name + '_history')}"
        yield (f"CREATE OR REPLACE VIEW {history} AS " + " UNION ALL ".join(
            f"SELECT *, ValidFrom_str AS valid_from, ValidTo_str AS valid_to, "
            f"'{source}' AS _version_source FROM {bronze}.{q(name + suffix)}"
            for source, suffix in (("current", ""), ("archive", "_archive"))))
        yield (f"CREATE OR REPLACE FUNCTION {silver}.{q(name + '_asof')}(t STRING) RETURNS TABLE "
               f"RETURN SELECT * FROM {history} WHERE valid_from <= {checked_time('t')} AND valid_to > t")
    if not tables:
        raise ValueError("No temporal table pairs in spec")
    yield (f"CREATE OR REPLACE VIEW {silver}.`temporal_change_events` AS SELECT DISTINCT "
           "source_table, valid_from FROM (" + " UNION ALL ".join(
               f"SELECT '{t['table']}' AS source_table, valid_from FROM "
               f"{silver}.{q(t['table'] + '_history')}" for t in tables) + ")")
    yield (f"CREATE OR REPLACE FUNCTION {silver}.`change_events`"
           "(tables ARRAY<STRING>, last_cutoff STRING, new_cutoff STRING) RETURNS TABLE RETURN "
           f"SELECT DISTINCT valid_from FROM {silver}.`temporal_change_events` "
           "WHERE array_contains(tables, source_table) "
           f"AND valid_from > {checked_time('last_cutoff')} "
           f"AND valid_from <= {checked_time('new_cutoff')} ORDER BY valid_from")


def known_checks(drop):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError(f"Unsupported smoke drop: {drop}")
    old = "2016-05-31 23:14:00.0000000"
    new = "2016-06-14 08:00:00.0000000"
    return [("application_people", "PersonID", 11, SAMPLE_TIMES[1], old,
             "archive" if drop == "D2" else "current"),
            ("application_people", "PersonID", 11, SAMPLE_TIMES[2],
             new if drop == "D2" else old, "current"),
            ("warehouse_stock_items", "StockItemID", 1, SAMPLE_TIMES[2],
             "2016-05-31 23:11:00.0000000", "current")]


def invariant_queries(spec, prefix):
    for table in temporal_tables(spec):
        name = table['table']
        keys = ', '.join(q(k) for k in table['primary_key'])
        yield (f"SELECT count(*) AS n FROM (SELECT valid_from, valid_to, "
               f"max(valid_to) OVER (PARTITION BY {keys} ORDER BY valid_from, valid_to "
               f"ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS previous_end "
               f"FROM {prefix}.{q(name + '_history')} WHERE valid_from <> valid_to) "
               "WHERE valid_from > valid_to OR previous_end > valid_from")
        for t in SAMPLE_TIMES:
            yield (f"SELECT count(*) AS n FROM (SELECT {keys} FROM "
                   f"{prefix}.{q(name + '_asof')}('{t}') GROUP BY {keys} HAVING count(*) > 1)")


def smoke(spark, spec, catalog, schema, drop):
    prefix = f"{q(catalog)}.{q(schema)}"
    for name, key, value, t, start, source in known_checks(drop):
        rows = spark.sql(f"SELECT valid_from, _version_source FROM {prefix}.{q(name + '_asof')}"
                         f"('{t}') WHERE {q(key)} = {value}").collect()
        if len(rows) != 1 or rows[0]['valid_from'] != start or rows[0]['_version_source'] != source:
            raise RuntimeError(f"{drop} AS OF smoke failed: {name} {value} at {t}: {rows}")
    for query in invariant_queries(spec, prefix):
        if spark.sql(query).collect()[0]['n']:
            raise RuntimeError(f"{drop} temporal invariant failed: {query}")
    print(f"PASS {drop}: known rows, AS OF uniqueness and history non-overlap")


def main(argv=None):
    parser = argparse.ArgumentParser()
    for name in ('catalog', 'bronze-schema', 'silver-schema', 'spec', 'drop'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--last-cutoff', default='')
    parser.add_argument('--new-cutoff', default='')
    args = parser.parse_args(argv)
    known_checks(args.drop)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spec = json.loads(Path(args.spec).read_text())
    for sql in statements(spec, args.catalog, args.bronze_schema, args.silver_schema):
        spark.sql(sql)
    smoke(spark, spec, args.catalog, args.silver_schema, args.drop)


if __name__ == '__main__':
    main()
