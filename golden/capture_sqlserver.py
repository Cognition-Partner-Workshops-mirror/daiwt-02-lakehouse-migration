"""Capture native WWI SQL Server goldens; mutations require explicit subcommands."""
import argparse
import csv
import datetime as dt
import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

OLTP = "WideWorldImporters"
DW = "WideWorldImportersDW"
LEGACY = Path.home() / "repos/daiwt-02-enterprise-etl"
PAGE_SIZE = 50_000
FULL_ROOT = Path.home() / "golden-work/full"
ENTITIES = (
    ("City", "City", "Dimension.City"),
    ("Customer", "Customer", "Dimension.Customer"),
    ("Employee", "Employee", "Dimension.Employee"),
    ("Payment Method", "PaymentMethod", "Dimension.Payment Method"),
    ("Stock Item", "StockItem", "Dimension.Stock Item"),
    ("Supplier", "Supplier", "Dimension.Supplier"),
    ("Transaction Type", "TransactionType", "Dimension.Transaction Type"),
    ("Movement", "Movement", "Fact.Movement"),
    ("Order", "Order", "Fact.Order"),
    ("Purchase", "Purchase", "Fact.Purchase"),
    ("Sale", "Sale", "Fact.Sale"),
    ("Stock Holding", "StockHolding", "Fact.Stock Holding"),
    ("Transaction", "Transaction", "Fact.Transaction"),
)
CLEAR_SQL = {
    key: f"DELETE FROM Integration.{key}_Staging;" for _, key, _ in ENTITIES
}
CLEAR_SQL["Purchase"] = "DELETE FROM Integration.Order_Staging;"
REPORTS = {
    "sale-by-day": (
        "SELECT [Invoice Date Key],COUNT_BIG(*) AS SaleRows,"
        "SUM([Total Including Tax]) AS TotalIncludingTax FROM Fact.Sale "
        "GROUP BY [Invoice Date Key] ORDER BY [Invoice Date Key]"
    ),
    "customer-history": (
        "SELECT [Customer Key],[WWI Customer ID],Customer,[Valid From],[Valid To] "
        "FROM Dimension.Customer ORDER BY [Customer Key]"
    ),
}


def utcnow():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def quote(name):
    return "[" + name.replace("]", "]]") + "]"


def qualified(name):
    return ".".join(quote(part) for part in name.split("."))


def snake(name):
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    return re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()


def safe_name(name):
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", name)


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".partial")
    temp.write_text(json.dumps(value, indent=2, default=str) + "\n")
    temp.replace(path)


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def csv_rows(path, expected_columns):
    csv.field_size_limit(2**31 - 1)
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream)
        if next(reader, None) != expected_columns:
            raise ValueError(f"Header mismatch: {path}")
        for row in reader:
            if len(row) != len(expected_columns):
                raise ValueError(f"Row width mismatch: {path}")
            yield row


@contextmanager
def row_index():
    FULL_ROOT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="csv-index-", dir=FULL_ROOT.parent) as work:
        with sqlite3.connect(str(Path(work) / "rows.sqlite")) as index:
            index.execute("CREATE TABLE rows (key TEXT PRIMARY KEY, body TEXT, copies INTEGER, position INTEGER)")
            yield index


def row_key(row, headers, keys):
    return json.dumps([row[headers.index(k)] for k in keys], ensure_ascii=False)


def index_rows(index, path, headers, keys, multiset=False):
    for position, row in enumerate(csv_rows(path, headers)):
        key, body = row_key(row, headers, keys), json.dumps(row, ensure_ascii=False)
        try:
            index.execute("INSERT INTO rows VALUES (?,?,1,?)", (key, body, position))
        except sqlite3.IntegrityError:
            if not multiset:
                raise ValueError(f"Duplicate primary key in {path}: {key}") from None
            index.execute("UPDATE rows SET copies=copies+1 WHERE key=?", (key,))
    index.commit()


def drop_number(name):
    if not re.fullmatch(r"D(?:0|[1-9][0-9]*)", name):
        raise ValueError(f"Invalid drop name: {name}")
    return int(name[1:])


def captures_in(out):
    paths = [p for p in out.glob("capture-D*.json")
             if re.fullmatch(r"capture-D[0-9]+\.json", p.name)]
    return [json.loads(p.read_text()) for p in sorted(paths, key=lambda p: drop_number(p.stem[8:]))]


def capture_input(out, drop, table, headers, keys, rows, previous=None):
    filename = safe_name(OLTP + "." + table) + ".csv"
    relative = f"inputs/{drop}/{filename}"
    full_path = FULL_ROOT / drop / filename
    if full_path.exists():
        raise ValueError(f"Full scratch export already exists: {full_path}")
    previous_path = None
    if drop != "D0":
        if previous is None:
            raise ValueError(f"No previous full capture for {table}")
        previous_path, previous_meta = previous
        baseline = Path(previous_meta["full_path"])
        if not baseline.exists() or sha256(baseline) != previous_meta["full_sha256"]:
            raise ValueError(f"Previous full scratch export missing or changed: {baseline}")
        if previous_meta["columns"] != headers or previous_meta["primary_key"] != keys:
            raise ValueError(f"Input schema/primary key changed: {table}")
    full = write_csv(FULL_ROOT / drop, filename, headers, rows)
    common = {"database": OLTP, "source_table": table, "primary_key": keys,
              "key_columns": keys or headers, "key_semantics": "primary_key" if keys else "whole_row_multiset",
              "full_path": str(full_path), "full_row_count": full["row_count"],
              "full_sha256": full["sha256"], "previous_input": previous_path,
              "input_kind": "base" if drop == "D0" else "upsert"}
    if drop == "D0":
        (out / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(full_path, out / relative)
        return {relative: {**full, **common}}
    identity = keys or headers
    with row_index() as old, row_index() as current:
        index_rows(old, baseline, headers, identity, not keys)
        index_rows(current, full_path, headers, identity, not keys)
        def changed():
            for key, body, copies in current.execute("SELECT key,body,copies FROM rows ORDER BY position"):
                prior = old.execute("SELECT body,copies FROM rows WHERE key=?", (key,)).fetchone()
                count = copies if prior is None else (int(prior[0] != body) if keys else max(0, copies-prior[1]))
                for _ in range(count):
                    yield json.loads(body)
        result = {relative: write_csv(out, relative, headers, changed(), **common)}
        def deleted():
            for key, copies in old.execute("SELECT key,copies FROM rows ORDER BY position"):
                now = current.execute("SELECT copies FROM rows WHERE key=?", (key,)).fetchone()
                count = copies if now is None else (0 if keys else max(0, copies-now[0]))
                for _ in range(count):
                    yield json.loads(key)
        removed = deleted()
        first = next(removed, None)
        if first is not None:
            def deletion_rows():
                yield first
                yield from removed
            deleted_path = relative[:-4] + ".deleted.csv"
            result[deleted_path] = write_csv(out, deleted_path, identity, deletion_rows(),
                                            **{**common, "input_kind": "delete"})
    return result


def apply_input_chain(index, out, files, relative, seen=None):
    seen = set() if seen is None else seen
    if relative in seen:
        raise ValueError("Cycle in input delta chain")
    seen.add(relative)
    meta = files[relative]
    headers, keys = meta["columns"], meta["key_columns"]
    multiset = meta["key_semantics"] == "whole_row_multiset"
    previous = meta["previous_input"]
    if previous is None:
        if meta["input_kind"] != "base":
            raise ValueError(f"Delta has no base: {relative}")
        index_rows(index, out / relative, headers, keys, multiset)
        return
    prior = files[previous]
    if prior["columns"] != headers or prior["key_columns"] != keys or prior["key_semantics"] != meta["key_semantics"]:
        raise ValueError(f"Incompatible delta chain: {relative}")
    apply_input_chain(index, out, files, previous, seen)
    deletion = relative[:-4] + ".deleted.csv"
    if deletion in files:
        for key_row in csv_rows(out / deletion, keys):
            key = json.dumps(key_row, ensure_ascii=False)
            existing = index.execute("SELECT copies FROM rows WHERE key=?", (key,)).fetchone()
            if not existing:
                raise ValueError(f"Deleted key absent from base: {key}")
            if existing[0] == 1:
                index.execute("DELETE FROM rows WHERE key=?", (key,))
            else:
                index.execute("UPDATE rows SET copies=copies-1 WHERE key=?", (key,))
    for row in csv_rows(out / relative, headers):
        key, body = row_key(row, headers, keys), json.dumps(row, ensure_ascii=False)
        if multiset:
            index.execute("INSERT INTO rows VALUES (?,?,1,0) ON CONFLICT(key) DO UPDATE SET copies=copies+1", (key, body))
        else:
            index.execute("INSERT INTO rows VALUES (?,?,1,0) ON CONFLICT(key) DO UPDATE SET body=excluded.body", (key, body))
    index.commit()


def verify_reconstruction(out, files):
    for relative, meta in files.items():
        if not relative.startswith("inputs/") or meta.get("input_kind") == "delete":
            continue
        if "full_path" not in meta or not Path(meta["full_path"]).exists():
            print(f"RECONSTRUCT SKIP {relative}: full scratch export missing", flush=True)
            continue
        full_path = Path(meta["full_path"])
        if sha256(full_path) != meta["full_sha256"]:
            raise ValueError(f"Full scratch SHA256 mismatch: {relative}")
        with row_index() as index:
            apply_input_chain(index, out, files, relative)
            digest = hashlib.sha256()
            class Sink:
                def write(self, value):
                    digest.update(value.encode("utf-8"))
            writer = csv.writer(Sink(), lineterminator="\n")
            writer.writerow(meta["columns"])
            count = 0
            for row in csv_rows(full_path, meta["columns"]):
                key = row_key(row, meta["columns"], meta["key_columns"])
                existing = index.execute("SELECT body,copies FROM rows WHERE key=?", (key,)).fetchone()
                if existing is None or json.loads(existing[0]) != row:
                    raise ValueError(f"Reconstruction content mismatch: {relative}")
                writer.writerow(json.loads(existing[0]))
                if existing[1] == 1:
                    index.execute("DELETE FROM rows WHERE key=?", (key,))
                else:
                    index.execute("UPDATE rows SET copies=copies-1 WHERE key=?", (key,))
                count += 1
            if index.execute("SELECT COUNT(*) FROM rows").fetchone()[0] or count != meta["full_row_count"]:
                raise ValueError(f"Reconstruction row count mismatch: {relative}")
            if digest.hexdigest() != meta["full_sha256"]:
                raise ValueError(f"Reconstructed SHA256 mismatch: {relative}")
        print(f"RECONSTRUCT PASS {relative}: full_rows={count} SHA256=OK", flush=True)


class Estate:
    def __init__(self):
        import boto3
        self.secret = json.loads(boto3.client("secretsmanager", region_name="us-east-1")
                                 .get_secret_value(SecretId="daiwt-02-sqlserver")["SecretString"])

    def connect(self, database):
        import pymssql
        return pymssql.connect(
            server=self.secret["host"], port=str(self.secret["port"]),
            user=self.secret["username"], password=self.secret["password"],
            database=database, encryption="require", autocommit=True,
            login_timeout=30, timeout=1800, as_dict=True,
        )


def query(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return list(cursor.fetchall())


def execute(connection, sql, params=None):
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        while cursor.nextset():
            pass


def columns(connection, table):
    return query(connection, """
        SELECT c.name, ty.name AS type_name, c.column_id
        FROM sys.columns c JOIN sys.types ty ON ty.user_type_id=c.user_type_id
        WHERE c.object_id=OBJECT_ID(%s) ORDER BY c.column_id
    """, (table,))


def primary_key(connection, table):
    return [r["name"] for r in query(connection, """
        SELECT c.name FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id=i.object_id AND ic.index_id=i.index_id
        JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
        WHERE i.is_primary_key=1 AND i.object_id=OBJECT_ID(%s)
        ORDER BY ic.key_ordinal
    """, (table,))]


def format_sql(expression, type_name):
    if type_name in ("geography", "geometry"):
        return f"{expression}.STAsText()"
    if type_name in ("binary", "varbinary", "image", "timestamp", "rowversion"):
        return f"CONVERT(varchar(max), {expression}, 2)"
    if type_name == "date":
        return f"CONVERT(varchar(10), {expression}, 23)"
    if type_name in ("datetime", "datetime2", "smalldatetime"):
        return f"CONVERT(varchar(27), {expression}, 121)"
    if type_name == "datetimeoffset":
        return f"CONVERT(varchar(40), {expression}, 121)"
    if type_name == "time":
        return f"CONVERT(varchar(20), {expression}, 121)"
    if type_name in ("decimal", "numeric", "money", "smallmoney"):
        return f"CONVERT(varchar(50), {expression})"
    if type_name in ("float", "real"):
        return f"CONVERT(varchar(50), {expression}, 3)"
    if type_name in ("text", "ntext", "xml"):
        return f"CONVERT(nvarchar(max), {expression})"
    return f"CONVERT(nvarchar(max), {expression})"


def keyset(keys, last):
    terms, params = [], []
    for i, key in enumerate(keys):
        terms.append("(" + " AND ".join(
            [f"{quote(k)} = %s" for k in keys[:i]] + [f"{quote(key)} > %s"]
        ) + ")")
        params.extend(last[:i + 1])
    return " OR ".join(terms), tuple(params)


def table_rows(connection, table, cols, page_size=PAGE_SIZE):
    if not cols:
        raise ValueError(f"Missing table or invisible columns: {table}")
    keys = primary_key(connection, table)
    by_name = {c["name"]: c for c in cols}
    projection = [format_sql("src." + quote(c["name"]), c["type_name"]) + " AS " + quote(c["name"])
                  for c in cols]
    if not keys:
        order = [format_sql("src." + quote(c["name"]), c["type_name"])
                 if c["type_name"] in ("geography", "geometry", "text", "ntext", "xml")
                 else "src." + quote(c["name"]) for c in cols]
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT {', '.join(projection)} FROM {qualified(table)} AS src "
                           f"ORDER BY {', '.join(order)}")
            while True:
                rows = cursor.fetchmany(page_size)
                if not rows:
                    break
                for row in rows:
                    yield [row[c["name"]] for c in cols]
        return
    hidden = [format_sql("src." + quote(k), by_name[k]["type_name"]) + f" AS [__key_{i}]"
              for i, k in enumerate(keys)]
    last = None
    while True:
        where, params = keyset(keys, last) if last is not None else ("", ())
        sql = (f"SELECT TOP ({int(page_size)}) {', '.join(projection + hidden)} "
               f"FROM {qualified(table)} AS src" + (f" WHERE {where}" if where else "")
               + " ORDER BY " + ", ".join("src." + quote(k) for k in keys))
        rows = query(connection, sql, params)
        for row in rows:
            yield [row[c["name"]] for c in cols]
        if len(rows) < page_size:
            break
        last = [rows[-1][f"__key_{i}"] for i in range(len(keys))]


def write_csv(root, relative, headers, rows, **metadata):
    if len(set(headers)) != len(headers):
        raise ValueError(f"Duplicate CSV columns: {relative}")
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".csv.partial")
    count = 0
    with temp.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(headers)
        for row in rows:
            if len(row) != len(headers):
                raise ValueError(f"CSV row width mismatch: {relative}")
            writer.writerow(["\\N" if value is None else value for value in row])
            count += 1
    temp.replace(path)
    result = {"columns": headers, "row_count": count, "size_bytes": path.stat().st_size,
              "sha256": sha256(path), **metadata}
    print(f"{relative}: rows={count} bytes={result['size_bytes']}", flush=True)
    return result


def input_tables(connection):
    catalog = {(r["schema_name"], r["name"]) for r in query(connection, """
        SELECT s.name AS schema_name,t.name FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id=t.schema_id
    """)}
    result = set()
    identifier = r"(?:\[[^\]]+\]|[A-Za-z_][\w$]*)"
    pattern = rf"\b(?:FROM|JOIN)\s+({identifier})\s*\.\s*({identifier})"
    for _, key, _ in ENTITIES:
        name = "Integration.Get" + key + "Updates"
        rows = query(connection, "SELECT OBJECT_DEFINITION(OBJECT_ID(%s)) AS definition", (name,))
        definition = rows[0]["definition"]
        if not definition:
            raise ValueError(f"Missing extract definition: {name}")
        references = {(s.strip("[]"), t.strip("[]"))
                      for s, t in re.findall(pattern, definition, re.I)}
        dependencies = query(connection, """
            SELECT referenced_schema_name AS schema_name,referenced_entity_name AS name
            FROM sys.sql_expression_dependencies WHERE referencing_id=OBJECT_ID(%s)
              AND referenced_database_name IS NULL AND referenced_schema_name IS NOT NULL
        """, (name,))
        references.update((r["schema_name"], r["name"]) for r in dependencies)
        missing = references - catalog
        if missing:
            raise ValueError(f"Unresolved extract source for {name}: {sorted(missing)}")
        result.update(references)
    history = query(connection, """
        SELECT s.name AS schema_name,t.name,hs.name AS history_schema,h.name AS history_name
        FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
        JOIN sys.tables h ON h.object_id=t.history_table_id
        JOIN sys.schemas hs ON hs.schema_id=h.schema_id
    """)
    result.update((r["history_schema"], r["history_name"]) for r in history
                  if (r["schema_name"], r["name"]) in result)
    return sorted(".".join(parts) for parts in result)


def legacy_commit():
    return subprocess.check_output(["git", "-C", str(LEGACY), "rev-parse", "HEAD"], text=True).strip()


def cutoffs(connection):
    return query(connection, "SELECT [Table Name],CONVERT(varchar(27),[Cutoff Time],121) "
                 "AS [Cutoff Time] FROM Integration.[ETL Cutoff] ORDER BY [Table Name]")


def run_state():
    path = Path(__file__).resolve().parent / ".capture_state.json"
    return path, json.loads(path.read_text()) if path.exists() else {}


def export(estate, drop, out, phase="all"):
    started = time.monotonic()
    marker = out / f"capture-{drop}.json"
    pending = out / f".capture-{drop}-inputs.json"
    if marker.exists() or (out / "outputs" / drop).exists() or (phase != "outputs" and (out / "inputs" / drop).exists()):
        raise ValueError(f"Drop {drop} already exists; use a fresh output directory")
    if phase == "outputs" and not pending.exists():
        raise ValueError(f"Export inputs before outputs for {drop}")
    previous_files = {}
    if drop_number(drop) and phase != "outputs":
        previous = [c for c in captures_in(out) if drop_number(c["drop"]["name"]) < drop_number(drop)]
        if not previous:
            raise ValueError("Delta export requires a previous completed drop and its full scratch exports")
        previous_files = {m["source_table"]: (p, m) for p, m in previous[-1]["files"].items()
                          if p.startswith("inputs/") and m.get("input_kind") != "delete"}
        for path, metadata in previous_files.values():
            full = Path(metadata.get("full_path", ""))
            if not full.is_file() or sha256(full) != metadata.get("full_sha256"):
                raise ValueError(f"Previous full scratch export missing or changed: {path}")
    files = json.loads(pending.read_text())["files"] if phase == "outputs" else {}
    with estate.connect(OLTP) as source, estate.connect(DW) as destination:
        version = query(source, "SELECT @@VERSION AS version, "
                        "CAST(SERVERPROPERTY('Edition') AS nvarchar(128)) AS edition")[0]
        before = cutoffs(destination)
        provenance = {"engine": "SQL Server", **version, "server_host": estate.secret["host"],
                      "databases": [OLTP, DW],
                      "legacy_repo": "Cognition-Partner-Workshops-mirror/daiwt-02-enterprise-etl",
                      "legacy_commit": legacy_commit()}
        for table in input_tables(source) if phase != "outputs" else []:
            cols = columns(source, table)
            files.update(capture_input(out, drop, table, [c["name"] for c in cols], primary_key(source, table),
                                       table_rows(source, table, cols), previous_files.get(table)))
        targets = [] if phase == "inputs" else [r["table_name"] for r in query(destination, """
            SELECT s.name+'.'+t.name AS table_name FROM sys.tables t
            JOIN sys.schemas s ON s.schema_id=t.schema_id
            WHERE s.name IN ('Fact','Dimension') OR
                (s.name='Integration' AND t.name IN ('Lineage','ETL Cutoff'))
            ORDER BY s.name,t.name
        """)]
        for table in targets:
            cols = columns(destination, table)
            relative = f"outputs/{drop}/tables/{safe_name(table)}.csv"
            files[relative] = write_csv(out, relative, [snake(c["name"]) for c in cols],
                                       table_rows(destination, table, cols), database=DW, source_table=table,
                                       source_columns=[c["name"] for c in cols])
        for name, sql in REPORTS.items() if phase != "inputs" else []:
            metadata = query(destination, """
                SELECT name,system_type_name FROM sys.dm_exec_describe_first_result_set(%s,NULL,0)
                WHERE is_hidden=0 ORDER BY column_ordinal
            """, (sql,))
            expressions = [format_sql(quote(c["name"]), c["system_type_name"].split("(")[0])
                           + " AS " + quote(c["name"]) for c in metadata]
            body, order = sql.rsplit(" ORDER BY ", 1)
            formatted = "SELECT " + ",".join(expressions) + f" FROM ({body}) AS report ORDER BY {order}"
            rows = query(destination, formatted)
            relative = f"outputs/{drop}/reports/{name}.csv"
            files[relative] = write_csv(out, relative, [snake(c["name"]) for c in metadata],
                                       ([row[c["name"]] for c in metadata] for row in rows), report=name,
                                       source_columns=[c["name"] for c in metadata])
        after = cutoffs(destination)
        if before != after:
            raise RuntimeError("ETL cutoffs changed during export; capture is not stable")
    _, state = run_state()
    etl_state = state.get("etl", {})
    applies = etl_state.get("new_cutoff") and all(
        r["Cutoff Time"].startswith(etl_state["new_cutoff"]) for r in after if r["Table Name"] != "Date")
    info = {"name": drop, "window_start": state.get("advance", {}).get("start") if applies or phase == "inputs" else None,
            "window_end": state.get("advance", {}).get("end") if applies or phase == "inputs" else None,
            "last_cutoff": etl_state.get("last_cutoff") if applies else after,
            "new_cutoff": etl_state.get("new_cutoff") if applies else after}
    write_json(pending if phase == "inputs" else marker, {"source": provenance, "captured_at_utc": utcnow(), "drop": info,
                        "files": files, "runtime_seconds": round(time.monotonic() - started, 3)})
    print(f"EXPORT {drop} phase={phase} complete: files={len(files)} runtime_seconds={time.monotonic()-started:.3f}")


def manifest(out):
    captures = captures_in(out)
    if not captures:
        raise ValueError("No completed drop captures")
    files = {}
    for capture in captures:
        for relative, metadata in capture["files"].items():
            if sha256(out / relative) != metadata["sha256"]:
                raise ValueError(f"Capture changed before manifest: {relative}")
            files[relative] = metadata
    last = captures[-1]["drop"]["name"]
    tables = []
    for path, metadata in files.items():
        if not path.startswith(f"outputs/{last}/"):
            continue
        if "report" in metadata:
            table = "report_" + snake(metadata["report"])
        else:
            schema, name = metadata["source_table"].split(".")
            if schema == "Integration":
                continue
            table = ("dim_" if schema == "Dimension" else "fact_") + snake(name)
        tables.append({"table": "gold." + table, "path": path, "columns": metadata["columns"]})
    result = {"source": captures[0]["source"], "capture_mode": "tsql",
              "captured_at_utc": captures[-1]["captured_at_utc"],
              "drops": [c["drop"] for c in captures], "batch_semantics": "cumulative",
              "input_semantics": {
                  "D0": "full base", "later_drops": "inserted/changed whole rows versus previous full export",
                  "matching": "primary key; tables without a PK use whole-row multiset identity",
                  "deletions": "<table>.deleted.csv contains disappeared PKs; no-PK tables contain full rows with multiplicity",
                  "unchanged": "header-only upsert file for every unchanged table",
                  "full_exports": "scratch outside repo: ~/golden-work/full/<drop>/<db.schema.table>.csv",
                  "reconstruction": "apply deletes and upserts cumulatively; verify in full scratch row order against full_sha256",
                  "outputs": "full every drop"},
              "csv_conventions": {"encoding": "UTF-8", "header": True, "line_ending": "LF",
                  "null": "\\N", "delimiter": ",", "quotechar": '"',
                  "ordering": "primary key; otherwise all columns", "inputs": "D0 full; later drops deltas",
                  "output_headers": "snake_case (source_columns preserves SQL names)",
                  "values": "SQL Server CONVERT: datetime style 121, date 23, decimal/money varchar(50), spatial STAsText, binary hex style 2, bit 0/1"},
              "nondeterministic_columns": ["*.lineage_key", "Integration.Lineage.data_load_started",
                  "Integration.Lineage.data_load_completed", "temporal.ValidFrom", "temporal.ValidTo"],
              "skipped_branches": json.loads(Path(__file__).with_name("ssis_branches.json").read_text()),
              "files": files, "tables": tables}
    write_json(out / "manifest.json", result)
    print(f"MANIFEST: drops={len(captures)} files={len(files)} parity_tables={len(tables)} last_drop={last}")
    return result


def verify(estate, out):
    csv.field_size_limit(2**31 - 1)
    data = json.loads((out / "manifest.json").read_text())
    if not data["drops"] or not data["files"]:
        raise ValueError("Empty capture manifest")
    actual = {str(p.relative_to(out)) for folder in ("inputs", "outputs") for p in (out / folder).rglob("*.csv")}
    if actual != set(data["files"]):
        raise ValueError("Manifest CSV inventory differs from disk")
    last = data["drops"][-1]["name"]
    with estate.connect(DW) as connection:
        for relative, metadata in data["files"].items():
            path = out / relative
            if sha256(path) != metadata["sha256"]:
                raise ValueError(f"SHA256 mismatch: {relative}")
            with path.open(encoding="utf-8", newline="") as stream:
                reader = csv.reader(stream)
                if next(reader) != metadata["columns"]:
                    raise ValueError(f"Header mismatch: {relative}")
                count = 0
                for row in reader:
                    if len(row) != len(metadata["columns"]):
                        raise ValueError(f"Row width mismatch: {relative}")
                    count += 1
            if count != metadata["row_count"]:
                raise ValueError(f"Row count mismatch: {relative}")
            if relative.startswith(f"outputs/{last}/tables/"):
                live = query(connection, f"SELECT COUNT_BIG(*) AS n FROM {qualified(metadata['source_table'])}")[0]["n"]
                if live != count:
                    raise ValueError(f"Live count mismatch: {relative}: CSV={count} live={live}")
                print(f"COUNT_BIG {relative}: CSV={count} live={live} MATCH", flush=True)
            print(f"VERIFY {relative}: rows={count} bytes={path.stat().st_size} SHA256=OK", flush=True)
    verify_reconstruction(out, data["files"])
    print(f"VERIFY PASS: files={len(data['files'])}; last-drop table counts match live COUNT_BIG")


def advance(estate, start, end):
    if start > end or start < dt.date(2013, 1, 1) or end > dt.date.today():
        raise ValueError("Window must be ordered, in 2013 or later, and not in the future")
    with estate.connect(OLTP) as connection:
        execute(connection, "EXEC DataLoadSimulation.Configuration_ApplyDataLoadSimulationProcedures;")
        execute(connection, """
            EXEC DataLoadSimulation.DailyProcessToCreateHistory
                @StartDate=%s,@EndDate=%s,@AverageNumberOfCustomerOrdersPerDay=60,
                @SaturdayPercentageOfNormalWorkDay=25,@SundayPercentageOfNormalWorkDay=0,
                @UpdateCustomFields=0,@IsSilentMode=0,@AreDatesPrinted=1;
        """, (start, end))
        execute(connection, "EXEC DataLoadSimulation.Configuration_RemoveDataLoadSimulationProcedures;")
        activity = {}
        for table, column in (("Sales.Orders", "OrderDate"), ("Sales.Invoices", "InvoiceDate"),
                              ("Purchasing.PurchaseOrders", "OrderDate"),
                              ("Warehouse.StockItemTransactions", "TransactionOccurredWhen")):
            activity[table] = query(connection, f"SELECT COUNT_BIG(*) AS n FROM {qualified(table)} "
                                    f"WHERE {quote(column)} >= %s AND {quote(column)} < DATEADD(day,1,%s)",
                                    (start, end))[0]["n"]
        print("GENERATOR ACTIVITY " + json.dumps(activity), flush=True)
        if not any(activity.values()):
            raise RuntimeError("Generator produced zero OLTP activity in the requested window")
    path, state = run_state()
    state["advance"] = {"start": str(start), "end": str(end), "activity": activity, "completed_at_utc": utcnow()}
    state.pop("etl", None)
    write_json(path, state)
    print(f"ADVANCE complete: {start} through {end}")


def result_columns(connection, key):
    sql = json.loads(Path(__file__).with_name("native_result_sets.json").read_text())[key]
    return query(connection, """
        SELECT name,error_number,error_message FROM sys.dm_exec_describe_first_result_set(
            %s,N'@LastCutoff datetime2(7), @NewCutoff datetime2(7)',0)
        WHERE is_hidden=0 OR error_number IS NOT NULL ORDER BY column_ordinal
    """, (sql,))


def destination_columns(key, names, stage):
    mapping = json.loads(Path(__file__).with_name("native_destination_columns.json").read_text()).get(key, {})
    destinations = [mapping.get(name, name) for name in names]
    if len(set(destinations)) != len(destinations) or any(name not in stage for name in destinations):
        raise ValueError(f"Result/staging columns differ: {key}")
    return destinations


def etl(estate, cutoff):
    with estate.connect(OLTP) as source, estate.connect(DW) as destination:
        matched = {}
        for _, key, _ in ENTITIES:
            result = result_columns(source, key)
            if not result or any(r["error_number"] for r in result):
                raise ValueError(f"Cannot describe extract result for {key}: {result}")
            stage = {c["name"] for c in columns(destination, "Integration." + key + "_Staging")}
            names = [r["name"] for r in result]
            matched[key] = destination_columns(key, names, stage)
        before = cutoffs(destination)
        execute(destination, "EXEC Integration.PopulateDateDimensionForYear %s;", (cutoff.year,))
        lineage_keys = []
        extracted = {}
        for table_name, key, _ in ENTITIES:
            lineage = query(destination, "EXEC Integration.GetLineageKey %s, %s;", (table_name, cutoff))
            lineage_keys.append(lineage[0]["LineageKey"])
            execute(destination, CLEAR_SQL[key])
            last = query(destination, "EXEC Integration.GetLastETLCutoffTime %s;", (table_name,))[0]["CutoffTime"]
            args = "" if key == "StockHolding" else " @LastCutoff=%s, @NewCutoff=%s"
            sql = (f"INSERT INTO {qualified(DW + '.Integration.' + key + '_Staging')} "
                   f"({', '.join(quote(n) for n in matched[key])}) "
                   f"EXEC {qualified(OLTP + '.Integration.Get' + key + 'Updates')}{args};")
            extracted[key] = query(destination, "SET NOCOUNT ON; " + sql + " SELECT @@ROWCOUNT AS n;",
                                   () if key == "StockHolding" else (last, cutoff))[0]["n"]
            print(f"EXTRACT {key}: rows={extracted[key]}", flush=True)
            execute(destination, f"EXEC Integration.MigrateStaged{key}Data;")
        lineage = query(destination, "SELECT * FROM Integration.Lineage WHERE [Lineage Key] IN ("
                        + ",".join(["%s"] * len(lineage_keys)) + ") ORDER BY [Lineage Key]", tuple(lineage_keys))
        print(json.dumps(lineage, indent=2, default=str))
        if len(lineage) != len(ENTITIES) or any(not row["Was Successful"] or row["Data Load Completed"] is None
                                               for row in lineage):
            raise RuntimeError("ETL lineage verification failed")
        if not any(n for key, n in extracted.items() if key != "StockHolding"):
            raise RuntimeError("ETL extracted zero incremental rows")
    path, state = run_state()
    state["etl"] = {"last_cutoff": before, "new_cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S"),
                    "lineage_keys": lineage_keys, "extracted_rows": extracted, "completed_at_utc": utcnow()}
    write_json(path, state)
    print("ETL PASS: all 13 lineage rows completed successfully")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("export", help="read-only full input/output capture")
    def drop_name(value):
        drop_number(value)
        return value
    capture.add_argument("--drop", type=drop_name, required=True)
    capture.add_argument("--out", type=Path, default=Path("golden"))
    capture.add_argument("--phase", choices=("all", "inputs", "outputs"), default="all")
    for name in ("manifest", "verify"):
        commands.add_parser(name).add_argument("--out", type=Path, default=Path("golden"))
    generator = commands.add_parser("advance", help="MUTATES OLTP using the estate simulation")
    generator.add_argument("--start", type=dt.date.fromisoformat, required=True)
    generator.add_argument("--end", type=dt.date.fromisoformat, required=True)
    load = commands.add_parser("etl", help="MUTATES DW by replaying the native package")
    load.add_argument("--new-cutoff", type=lambda s: dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"), required=True)
    args = parser.parse_args()
    if args.command == "manifest":
        manifest(args.out)
        return
    estate = Estate()
    if args.command == "export":
        export(estate, args.drop, args.out, args.phase)
    elif args.command == "verify":
        verify(estate, args.out)
    elif args.command == "advance":
        advance(estate, args.start, args.end)
    elif args.command == "etl":
        etl(estate, args.new_cutoff)


if __name__ == "__main__":
    main()
