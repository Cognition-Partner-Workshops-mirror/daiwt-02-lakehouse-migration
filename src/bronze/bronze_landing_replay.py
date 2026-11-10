"""Bronze landing and replay of the golden WWI input drops.

Per drop: land every input file append-only as raw STRING rows into `<table>_raw`
(`_drop`, `_op`, `_file`, `_ingested_at`), then maintain the typed current-state table
`<table>`: D0 replaces it, later drops apply `<file>.deleted.csv` (primary key, or one copy
per whole-row multiset match) and then the upserts. Every table is checked against the
manifest `full_row_count` and `full_sha256` (snapshot rendered back to golden CSV form).
"""
import argparse
import csv
import hashlib
import io
import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

NULL = "\\N"
META_COLUMNS = ("_drop", "_file", "_ingested_at")
DROP_PATTERN = re.compile(r"D[0-9]+")
NEEDS_QUOTES = r"[,\"\r\n]"
NULL_LITERAL = "'\\\\N'"
LOG_TABLE = "bronze_replay_log"
DROPS_TABLE = "golden_drops"


def q(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name}")
    return f"`{name}`"


def drop_index(drop):
    if not DROP_PATTERN.fullmatch(drop):
        raise ValueError(f"Invalid drop: {drop}")
    return int(drop[1:])


def owned_tables(spec):
    names = [t["table"] for t in spec["tables"]] + [t["table"] + "_raw" for t in spec["tables"]]
    return sorted(f"bronze.{n}" for n in names + [LOG_TABLE, DROPS_TABLE])


def deleted_path(relative):
    return relative[:-4] + ".deleted.csv"


def parse_csv(text, columns):
    csv.field_size_limit(2**31 - 1)
    reader = csv.reader(io.StringIO(text, newline=""))
    if next(reader, None) != columns:
        raise ValueError("CSV header does not match manifest columns")
    rows = []
    for row in reader:
        if len(row) != len(columns):
            raise ValueError("CSV row width mismatch")
        rows.append([None if v == NULL else v for v in row])
    return rows


def render_csv(columns, rows):
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows([NULL if v is None else v for v in row] for row in rows)
    return out.getvalue()


# Typed projection and golden rendering, shared by the Spark job and the offline reference.

def typed_select(table):
    exprs = []
    for c in table["columns"]:
        name = q(c["name"])
        exprs.append(f"CAST({name} AS {c['type']}) AS {name}")
        if c["type"] == "TIMESTAMP_NTZ":
            exprs.append(f"{name} AS {q(c['name'] + '_str')}")
    return exprs


def snapshot_columns(table):
    names = []
    for c in table["columns"]:
        names.append(c["name"])
        if c["type"] == "TIMESTAMP_NTZ":
            names.append(c["name"] + "_str")
    return names


def golden_value(c):
    name = q(c["name"])
    if c["type"] == "TIMESTAMP_NTZ":
        return q(c["name"] + "_str")
    if c["type"] == "BOOLEAN":
        return f"CAST(CAST({name} AS INT) AS STRING)"
    if c["type"] == "STRING":
        return name
    return f"CAST({name} AS STRING)"


def golden_line(table):
    fields = []
    for c in table["columns"]:
        v = f"coalesce({golden_value(c)}, {NULL_LITERAL})"
        fields.append(f"CASE WHEN {v} RLIKE '{NEEDS_QUOTES}' "
                      f"THEN concat('\"', replace({v}, '\"', '\"\"'), '\"') ELSE {v} END")
    return "concat_ws(',', " + ", ".join(fields) + ")"


def golden_order(table):
    """Row order of the full export: primary key, otherwise every column (case-insensitive text)."""
    by_name = {c["name"]: c for c in table["columns"]}
    if table["primary_key"]:
        return [f"{q(k)} ASC NULLS FIRST" for k in table["primary_key"]]
    order = []
    for c in by_name.values():
        if c["type"] == "STRING":
            order += [f"lower({q(c['name'])}) ASC NULLS FIRST", f"{q(c['name'])} ASC NULLS FIRST"]
        else:
            order.append(f"{golden_value(c) if c['type'] == 'TIMESTAMP_NTZ' else q(c['name'])} ASC NULLS FIRST")
    return order


def null_safe_join(table, left, right, columns=None):
    columns = columns or [c["name"] for c in table["columns"]]
    return " AND ".join(f"{left}.{q(c)} <=> {right}.{q(c)}" for c in columns)


def pk_delete_sql(target, table, deletes):
    keys = table["primary_key"]
    return (f"DELETE FROM {target} AS t WHERE EXISTS (SELECT 1 FROM {deletes} AS d WHERE "
            + null_safe_join(table, "t", "d", keys) + ")")


def pk_missing_deletes_sql(target, table, deletes):
    keys = table["primary_key"]
    return (f"SELECT count(*) AS n FROM {deletes} AS d LEFT ANTI JOIN {target} AS t ON "
            + null_safe_join(table, "t", "d", keys))


def multiset_missing_deletes_sql(target, table, deletes):
    cols = ", ".join(q(c) for c in snapshot_columns(table))
    on = null_safe_join(table, "s", "d", snapshot_columns(table))
    return (f"SELECT count(*) AS n FROM (SELECT {cols}, count(*) AS _n FROM {deletes} GROUP BY ALL) AS d "
            f"LEFT JOIN (SELECT {cols}, count(*) AS _n FROM {target} GROUP BY ALL) AS s ON {on} "
            f"WHERE coalesce(s._n, 0) < d._n")


def multiset_delete_sql(target, table, deletes):
    """Remove exactly one stored copy per deleted row (whole-row identity, with multiplicity)."""
    cols = ", ".join(q(c) for c in snapshot_columns(table))
    keep = ", ".join(f"s.{q(c)}" for c in snapshot_columns(table) + list(META_COLUMNS))
    on = null_safe_join(table, "s", "d", snapshot_columns(table))
    return (f"INSERT OVERWRITE {target} SELECT {keep} FROM "
            f"(SELECT *, row_number() OVER (PARTITION BY {cols} ORDER BY _drop, _file) AS _rn FROM {target}) AS s "
            f"LEFT JOIN (SELECT {cols}, count(*) AS _n FROM {deletes} GROUP BY ALL) AS d ON {on} "
            f"WHERE s._rn > coalesce(d._n, 0)")


def merge_sql(target, table, upserts):
    on = null_safe_join(table, "t", "s", table["primary_key"])
    return (f"MERGE INTO {target} AS t USING {upserts} AS s ON {on} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")


def reference_replay(table, state, upserts, deletes):
    """Pure-Python statement of the replay semantics (used by offline tests)."""
    names = [c["name"] for c in table["columns"]]
    keys = [names.index(k) for k in table["primary_key"]]
    if keys:
        current = {tuple(r[i] for i in keys): r for r in state}
        for d in deletes:
            key = tuple(d)
            if key not in current:
                raise ValueError(f"Deleted key absent: {key}")
            del current[key]
        for r in upserts:
            current[tuple(r[i] for i in keys)] = r
        return list(current.values())
    remaining = list(state)
    for d in deletes:
        if d not in remaining:
            raise ValueError("Deleted row absent")
        remaining.remove(d)
    return remaining + list(upserts)


def reference_sort_key(table):
    def key(row):
        out = []
        for c, v in zip(table["columns"], row):
            if v is None:
                out.append((0,))
            elif c["type"] in ("INT", "BIGINT") or c["type"].startswith("DECIMAL"):
                out.append((1, float(v)))
            elif c["type"] == "STRING":
                out.append((1, v.lower(), v))
            else:
                out.append((1, v))
        return out
    if table["primary_key"]:
        idx = [[c["name"] for c in table["columns"]].index(k) for k in table["primary_key"]]
        return lambda row: [int(row[i]) for i in idx]
    return key


# Spark job.

class Replay:
    def __init__(self, spark, catalog, schema, golden_path, spec, drop, cutoffs):
        self.spark, self.spec, self.drop, self.cutoffs = spark, spec, drop, cutoffs
        self.prefix = f"{q(catalog)}.{q(schema)}"
        self.golden = Path(golden_path)
        self.manifest = json.loads((self.golden / "manifest.json").read_text())
        self.files = self.manifest["files"]
        self.ingested_at = datetime.now(timezone.utc)

    def fqn(self, name):
        return f"{self.prefix}.{q(name)}"

    def sql(self, statement):
        return self.spark.sql(statement)

    def read_input(self, relative, columns):
        data = (self.golden / relative).read_bytes()
        if hashlib.sha256(data).hexdigest() != self.files[relative]["sha256"]:
            raise ValueError(f"Golden input checksum mismatch: {relative}")
        rows = parse_csv(data.decode("utf-8"), columns)
        if len(rows) != self.files[relative]["row_count"]:
            raise ValueError(f"Golden input row count mismatch: {relative}")
        return rows

    def frame(self, rows, columns, relative, op):
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType, StructField, StructType
        schema = StructType([StructField(c, StringType(), True) for c in columns])
        df = self.spark.createDataFrame(rows, schema)
        return df.select(*columns, F.lit(self.drop).alias("_drop"), F.lit(op).alias("_op"),
                         F.lit(relative).alias("_file"), F.lit(self.ingested_at).alias("_ingested_at"))

    def guard(self):
        index = drop_index(self.drop)
        if self.drop not in {d["name"] for d in self.manifest["drops"]}:
            raise ValueError(f"Drop {self.drop} is not in the golden manifest")
        if index == 0:
            return
        applied = self.sql(f"SELECT drop, count_if(status = 'PASS') AS passed, count(*) AS n "
                           f"FROM {self.fqn(LOG_TABLE)} GROUP BY drop").collect()
        done = {r["drop"]: (r["passed"], r["n"]) for r in applied}
        previous = f"D{index - 1}"
        if self.drop in done:
            raise ValueError(f"Drop {self.drop} was already applied; replay from D0")
        if done.get(previous) != (len(self.spec["tables"]),) * 2 or any(drop_index(d) > index for d in done):
            raise ValueError(f"Drop {self.drop} requires {previous} to be the last fully applied drop")

    def reset(self):
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType
        rows = []
        for d in self.manifest["drops"]:
            last = {c["Table Name"]: c["Cutoff Time"] for c in d["last_cutoff"]}
            new = {c["Table Name"]: c["Cutoff Time"] for c in d["new_cutoff"]}
            for name in sorted(new):
                rows.append((d["name"], drop_index(d["name"]), d["window_start"], d["window_end"],
                             name, last.get(name), new[name]))
        schema = StructType([StructField(n, t, True) for n, t in [
            ("drop", StringType()), ("drop_index", IntegerType()), ("window_start", StringType()),
            ("window_end", StringType()), ("table_name", StringType()), ("last_cutoff", StringType()),
            ("new_cutoff", StringType())]])
        (self.spark.createDataFrame(rows, schema).write.mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(self.fqn(DROPS_TABLE)))
        self.sql(f"CREATE OR REPLACE TABLE {self.fqn(LOG_TABLE)} (drop STRING, drop_index INT, "
                 "source_table STRING, bronze_table STRING, input_file STRING, upsert_rows BIGINT, "
                 "deleted_rows BIGINT, expected_rows BIGINT, actual_rows BIGINT, expected_sha256 STRING, "
                 "actual_sha256 STRING, status STRING, last_cutoff STRING, new_cutoff STRING, "
                 "ingested_at TIMESTAMP)")

    def apply(self, table):
        names = [c["name"] for c in table["columns"]]
        relative = f"inputs/{self.drop}/{table['file']}"
        meta = self.files[relative]
        if meta["columns"] != names or meta["primary_key"] != table["primary_key"]:
            raise ValueError(f"Spec drift for {relative}")
        if self.drop != "D0" and meta["previous_input"] != f"inputs/D{drop_index(self.drop) - 1}/{table['file']}":
            raise ValueError(f"Unexpected delta chain for {relative}")
        raw, snap = self.fqn(table["table"] + "_raw"), self.fqn(table["table"])
        view = f"_bronze_{table['table']}_{self.drop}"
        upserts = self.read_input(relative, names)
        frames = [self.frame(upserts, names, relative, "base" if self.drop == "D0" else "upsert")]
        deletes = []
        if deleted_path(relative) in self.files:
            key_names = self.files[deleted_path(relative)]["columns"]
            deletes = self.read_input(deleted_path(relative), key_names)
            padded = [[r[key_names.index(n)] if n in key_names else None for n in names] for r in deletes]
            frames.append(self.frame(padded, names, deleted_path(relative), "delete"))
        landed = frames[0] if len(frames) == 1 else frames[0].unionByName(frames[1])
        if self.drop == "D0":
            landed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(raw)
        else:
            landed.write.mode("append").saveAsTable(raw)
        typed = ", ".join(typed_select(table) + [q(c) for c in META_COLUMNS])
        frames[0].createOrReplaceTempView(view)
        if self.drop == "D0":
            self.sql(f"CREATE OR REPLACE TABLE {snap} AS SELECT {typed} FROM {view}")
        else:
            if deletes:
                frames[1].createOrReplaceTempView(view + "_del")
                deleted = f"(SELECT {typed} FROM {view}_del)"
                if table["primary_key"]:
                    check, apply = pk_missing_deletes_sql(snap, table, deleted), pk_delete_sql(snap, table, deleted)
                else:
                    check, apply = (multiset_missing_deletes_sql(snap, table, deleted),
                                    multiset_delete_sql(snap, table, deleted))
                if self.sql(check).collect()[0]["n"]:
                    raise ValueError(f"Deleted rows absent from current state: {relative}")
                self.sql(apply)
            if upserts:
                source = f"(SELECT {typed} FROM {view})"
                if table["primary_key"]:
                    self.sql(merge_sql(snap, table, source))
                else:
                    self.sql(f"INSERT INTO {snap} SELECT {typed} FROM {view}")
        return self.verify(table, relative, meta, len(upserts), len(deletes))

    def verify(self, table, relative, meta, upsert_rows, deleted_rows):
        snap = self.fqn(table["table"])
        lines = (self.sql(f"SELECT {golden_line(table)} AS line FROM {snap} "
                          f"ORDER BY {', '.join(golden_order(table))}").toPandas()["line"])
        digest = hashlib.sha256((",".join(c["name"] for c in table["columns"]) + "\n").encode("utf-8"))
        for line in lines:
            digest.update((line + "\n").encode("utf-8"))
        actual_rows, actual_sha = len(lines), digest.hexdigest()
        ok = actual_rows == meta["full_row_count"] and actual_sha == meta["full_sha256"]
        return (self.drop, drop_index(self.drop), table["source_table"], f"bronze.{table['table']}", relative,
                upsert_rows, deleted_rows, meta["full_row_count"], actual_rows, meta["full_sha256"], actual_sha,
                "PASS" if ok else "FAIL", self.cutoffs[0], self.cutoffs[1], self.ingested_at)

    def run(self, workers=8):
        self.guard()
        if self.drop == "D0":
            self.reset()
        with ThreadPoolExecutor(workers) as pool:
            results = list(pool.map(self.apply, self.spec["tables"]))
        log = self.spark.table(self.fqn(LOG_TABLE))
        self.spark.createDataFrame(results, log.schema).write.mode("append").saveAsTable(self.fqn(LOG_TABLE))
        for r in results:
            print(f"{r[11]} {r[0]} {r[3]}: upserts={r[5]} deletes={r[6]} rows={r[8]}/{r[7]} "
                  f"sha256={'OK' if r[10] == r[9] else r[10]}", flush=True)
        failed = [r[3] for r in results if r[11] != "PASS"]
        if failed:
            raise RuntimeError(f"Bronze replay {self.drop} mismatches: {failed}")
        return results


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--golden-path", required=True)
    parser.add_argument("--spec", required=True)
    parser.add_argument("--drop", required=True)
    parser.add_argument("--last-cutoff", default="")
    parser.add_argument("--new-cutoff", default="")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    spec = json.loads(Path(args.spec).read_text())
    Replay(spark, args.catalog, args.schema, args.golden_path, spec, args.drop,
           (args.last_cutoff or None, args.new_cutoff or None)).run()


if __name__ == "__main__":
    main()
