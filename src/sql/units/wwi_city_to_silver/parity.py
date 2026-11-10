"""Strict per-drop parity of silver.city_changes against the golden Dimension.City outputs.

D0 is the restored baseline (seed), so its staged change set must be empty. For D(n), n > 0, the
staged rows must equal, as a multiset, the rows (by city_key) that golden Dimension.City gains from
D(n-1) to D(n), projected to the staged columns. Existing golden rows may only change by closing
an open version at the first staged Valid From of that city. Quarantine must be empty.
"""
import argparse
import collections
import csv
import hashlib
import json
import sys

GOLDEN_TABLE = "Dimension.City"
STAGED = ["wwi_city_id", "city", "state_province", "country", "continent", "sales_territory",
          "region", "subregion", "location", "latest_recorded_population", "valid_from", "valid_to"]
END_OF_TIME = "9999-12-31 23:59:59.9999999"
NULL = "\\N"


def read_golden(golden, manifest, drop):
    relative = f"outputs/{drop}/tables/{GOLDEN_TABLE}.csv"
    meta = manifest["files"][relative]
    with open(f"{golden}/{relative}", "rb") as stream:
        data = stream.read()
    if hashlib.sha256(data).hexdigest() != meta["sha256"]:
        raise ValueError(f"{relative} sha256 differs from manifest")
    csv.field_size_limit(sys.maxsize)
    reader = csv.reader(data.decode("utf-8").splitlines())
    header = next(reader)
    if header != meta["columns"] or header[1:13] != STAGED:
        raise ValueError(f"{relative} header differs from contract")
    rows = {row[0]: tuple(None if v == NULL else v for v in row[1:13]) for row in reader}
    if len(rows) != meta["row_count"]:
        raise ValueError(f"{relative} row count differs from manifest")
    return rows


def compare_drop(drop, previous, current, actual, quarantined):
    """previous/current: {city_key: staged tuple}; previous None for the seed drop."""
    actual = collections.Counter(actual)
    if previous is None:
        expected = collections.Counter()
        problems = []
    else:
        problems = [f"golden key {k} disappeared" for k in sorted(set(previous) - set(current))]
        expected = collections.Counter(current[k] for k in current if k not in previous)
        first_staged = {}
        for row in expected.elements():
            first_staged[row[0]] = min(first_staged.get(row[0], row[10]), row[10])
        for key in sorted(set(previous) & set(current)):
            before, after = previous[key], current[key]
            if before == after:
                if before[11] == END_OF_TIME and before[0] in first_staged:
                    problems.append(f"golden key {key} left open despite staged versions")
                continue
            if (before[:11] != after[:11] or before[11] != END_OF_TIME
                    or after[11] != first_staged.get(before[0])):
                problems.append(f"golden key {key} changed beyond close-off")
    missing, extra = expected - actual, actual - expected
    status = "PASS" if not (missing or extra or problems or quarantined) else "FAIL"
    return {
        "drop": drop, "status": status, "expected_rows": sum(expected.values()),
        "actual_rows": sum(actual.values()), "missing": sum(missing.values()),
        "extra": sum(extra.values()), "quarantined": quarantined, "problems": problems[:20],
        "missing_sample": [list(r) for r in list(missing)[:5]],
        "extra_sample": [list(r) for r in list(extra)[:5]],
    }


def datetime2(value):
    date, _, time = value.partition(" ")
    seconds, _, fraction = time.partition(".")
    return f"{date} {seconds}.{fraction.ljust(7, '0')}"


def main():
    parser = argparse.ArgumentParser()
    for name in ("catalog", "bronze-schema", "silver-schema", "golden-path", "drop"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--last-cutoff", default="")
    parser.add_argument("--new-cutoff", default="")
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    golden = args.golden_path.rstrip("/")
    with open(f"{golden}/manifest.json", encoding="utf-8") as stream:
        manifest = json.load(stream)
    bronze = f"`{args.catalog}`.`{args.bronze_schema}`"
    silver = f"`{args.catalog}`.`{args.silver_schema}`"
    names = [d["name"] for d in manifest["drops"]]
    landed = [r[0] for r in spark.sql(
        f"SELECT `drop` FROM {bronze}.bronze_replay_log WHERE status = 'PASS' "
        "GROUP BY `drop`, drop_index ORDER BY drop_index").collect()]
    if args.drop not in names or landed != names[:names.index(args.drop) + 1]:
        raise ValueError(f"Replayed drops {landed} do not end at {args.drop}")
    window = spark.sql(f"SELECT last_cutoff, new_cutoff FROM {bronze}.golden_drops "
                       f"WHERE `drop` = '{args.drop}' AND table_name = 'City'").collect()
    if args.last_cutoff and args.new_cutoff and (
            len(window) != 1 or [datetime2(v) for v in window[0]] != [args.last_cutoff, args.new_cutoff]):
        raise ValueError(f"City window {window} differs from runner cutoffs")
    columns = ", ".join(f"CAST({c} AS STRING)" for c in STAGED)
    results, previous = [], None
    for drop in landed:
        current = read_golden(golden, manifest, drop)
        actual = [tuple(r) for r in spark.sql(
            f"SELECT {columns} FROM {silver}.city_changes WHERE _drop = '{drop}'").collect()]
        quarantined = spark.sql(
            f"SELECT COUNT(*) FROM {silver}.city_changes_quarantine WHERE _drop = '{drop}'").first()[0]
        results.append(compare_drop(drop, previous, current, actual, quarantined))
        previous = current
    report = {"unit": "wwi_city_to_silver", "golden": GOLDEN_TABLE,
              "status": "PASS" if all(r["status"] == "PASS" for r in results) else "FAIL",
              "results": results}
    print(json.dumps(report, indent=2))
    if report["status"] != "PASS":
        raise SystemExit("City silver parity FAILED")


if __name__ == "__main__":
    main()
