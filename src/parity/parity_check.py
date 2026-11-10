"""Contract-driven, warehouse-side multiset parity against published CSVs."""
import argparse
import csv
import io
import json
import re
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def identifier(value):
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid identifier: {value}")
    return f"`{value}`"


# Retained for the offline SQL Server capture verification API, not warehouse parity.
class GoldenStore:
    def __init__(self, client, path):
        self.client, self.path = client, path.rstrip("/")

    def manifest(self):
        self.client.volumes.read("daiwt_02_mig_prod.landing.golden")
        response = self.client.files.download(self.path + "/manifest.json")
        with response.contents as stream:
            return json.load(stream)

    def rows(self, entry):
        path = entry["path"]
        if not path.startswith("outputs/") or ".." in path.split("/"):
            raise ValueError("Golden path must be under outputs/")
        response = self.client.files.download(self.path + "/" + path)
        with response.contents as stream:
            reader = csv.reader(io.StringIO(stream.read().decode("utf-8")))
            if next(reader) != entry["columns"]:
                raise ValueError("CSV header differs from manifest columns")
            rows = [[None if value == "\\N" else value for value in row] for row in reader]
        if any(len(row) != len(entry["columns"]) for row in rows):
            raise ValueError("CSV row width differs from manifest")
        return rows


def compare(manifest, registry, read_golden, read_actual):
    producers = {}
    for unit, spec in registry.items():
        for table in spec["outputs"]:
            if table in producers:
                raise ValueError(f"Multiple producers for {table}")
            producers[table] = unit
    results = []
    seen = set()
    for entry in manifest["tables"]:
        table = entry["table"]
        if table in seen:
            raise ValueError(f"Duplicate golden: {table}")
        seen.add(table)
        row = dict(table=table, status="SKIP", reason="not yet migrated")
        if table in producers:
            try:
                golden, actual = read_golden(entry), read_actual(entry)
                ok = Counter(map(tuple, golden)) == Counter(map(tuple, actual))
                row.update(unit=producers[table], status="PASS" if ok else "FAIL",
                           reason="matched" if ok else "row count or content mismatch")
            except Exception as exc:
                row.update(status="FAIL", reason=str(exc))
        results.append(row)
    return dict(status="FAIL" if any(r["status"] == "FAIL" for r in results) else "PASS",
                goldens=len(results), results=results)


def literal(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


# CSV formatting lives here until the P3 framework helpers land.
def render_column(column, data_type):
    col = identifier(column)
    kind = data_type.upper().split("(")[0].strip()
    if kind == "STRING":
        return col
    if kind in ("TIMESTAMP", "TIMESTAMP_NTZ"):
        return f"concat(date_format({col}, 'yyyy-MM-dd HH:mm:ss.SSSSSS'), '0')"
    if kind == "DATE":
        return f"date_format({col}, 'yyyy-MM-dd')"
    if kind == "BOOLEAN":
        return f"CASE WHEN {col} IS NULL THEN NULL WHEN {col} THEN '1' ELSE '0' END"
    if kind == "BINARY":
        return f"upper(hex({col}))"
    if kind in ("DECIMAL", "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE"):
        return f"CAST({col} AS STRING)"
    raise ValueError(f"Unsupported type for {column}: {data_type}")


def normalize(columns, comparators):
    if set(comparators) - set(columns):
        raise ValueError("Comparator column absent from contract columns")
    expressions = []
    for column in columns:
        col = identifier(column)
        rule = comparators.get(column, "strict")
        if rule == "strict":
            expressions.append(col)
            continue
        pattern = r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{7}$"
        predicate = f"{col} IS NOT NULL AND {col} RLIKE {literal(pattern)}"
        predicate += f" AND try_to_timestamp(substr({col}, 1, 19), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL"
        if rule.startswith("gte:") and rule[4:] in columns:
            ref = identifier(rule[4:])
            predicate += f" AND {ref} IS NOT NULL AND {ref} RLIKE {literal(pattern)} AND {col} >= {ref}"
            predicate += f" AND try_to_timestamp(substr({ref}, 1, 19), 'yyyy-MM-dd HH:mm:ss') IS NOT NULL"
        elif rule != "non_null_datetime7":
            raise ValueError(f"Unknown comparator: {rule}")
        expressions.append(f"CASE WHEN {predicate} THEN '<ts>' ELSE '<invalid>' END AS {col}")
    return ", ".join(expressions)


def select_entries(contract, drop):
    if drop not in ("D0", "D1", "D2"):
        raise ValueError(f"Unsupported drop: {drop}")
    outputs = contract["outputs"]
    entries = [e for e in contract["entries"] if e["drop"] == drop]
    if (len(outputs) != 18 or len(set(outputs)) != 18 or len(entries) != 18
            or {e["table"] for e in entries} != set(outputs)):
        raise ValueError("Drop must contain exactly the 18 contract outputs")
    for entry in entries:
        columns = entry["columns"]
        if not columns or len(set(columns)) != len(columns):
            raise ValueError("Invalid contract columns")
        normalize(columns, entry["compare"])
        if not entry["path"].startswith(f"outputs/{drop}/") or ".." in entry["path"].split("/"):
            raise ValueError("Invalid golden path")
    return entries


class Warehouse:
    def __init__(self, client, warehouse_id):
        self.client = client
        self.warehouse_id = warehouse_id

    def sql(self, statement):
        response = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=statement, wait_timeout="50s")
        while response.status.state.value in ("PENDING", "RUNNING"):
            time.sleep(1)
            response = self.client.statement_execution.get_statement(response.statement_id)
        if response.status.state.value != "SUCCEEDED":
            raise RuntimeError(str(response.status.error))
        rows = list(response.result.data_array or []) if response.result else []
        chunk = response.result.next_chunk_index if response.result else None
        while chunk is not None:
            result = self.client.statement_execution.get_statement_result_chunk_n(
                response.statement_id, chunk)
            rows.extend(result.data_array or [])
            chunk = result.next_chunk_index
        if response.manifest and response.manifest.truncated:
            raise RuntimeError("SQL result truncated")
        return rows


def compare_entry(warehouse, entry, golden_path, catalog, schema):
    table = entry["table"].split(".")[1]
    target = ".".join(identifier(v) for v in (catalog, schema, table))
    try:
        types = {row[0]: row[1] for row in warehouse.sql(f"DESCRIBE TABLE {target}")
                 if row[0] and not row[0].startswith("#")}
    except Exception as exc:
        if any(s in str(exc).upper() for s in ("TABLE_OR_VIEW_NOT_FOUND", "TABLE_NOT_FOUND", "DOES NOT EXIST")):
            raise RuntimeError(f"missing table: {target}") from exc
        raise
    columns = entry["columns"]
    rendered = ", ".join(f"{render_column(c, types[c])} AS {identifier(c)}" for c in columns)
    schema_string = ", ".join(f"{identifier(c)} STRING" for c in columns)
    source = (
        f"read_files({literal(golden_path.rstrip('/') + '/' + entry['path'])}, "
        f"format=>'csv', header=>true, schema=>{literal(schema_string)}, "
        f"nullValue=>{literal(chr(92) + 'N')}, multiLine=>true, escape=>{literal(chr(34))}, mode=>'FAILFAST')")
    normalized = normalize(columns, entry["compare"])
    cte = (f"WITH golden_raw AS (SELECT * FROM {source}), "
           f"actual_raw AS (SELECT {rendered} FROM {target}), "
           f"golden AS (SELECT {normalized} FROM golden_raw), "
           f"actual AS (SELECT {normalized} FROM actual_raw), "
           "missing AS (SELECT * FROM golden EXCEPT ALL SELECT * FROM actual), "
           "extra AS (SELECT * FROM actual EXCEPT ALL SELECT * FROM golden) ")
    invalid = " OR ".join(
        f"{identifier(c)} = '<invalid>'" for c, rule in entry["compare"].items()
        if rule != "strict") or "false"
    counts = warehouse.sql(cte + "SELECT (SELECT count(*) FROM golden), "
                           "(SELECT count(*) FROM actual), (SELECT count(*) FROM missing), "
                           "(SELECT count(*) FROM extra), "
                           f"(SELECT count(*) FROM golden WHERE {invalid}) + "
                           f"(SELECT count(*) FROM actual WHERE {invalid})")[0]
    golden, actual, missing, extra, invalid_rows = map(int, counts)
    ok = golden == entry["row_count"] and golden == actual and missing == extra == invalid_rows == 0
    result = dict(golden_rows=golden, actual_rows=actual, missing_rows=missing, extra_rows=extra,
                  status="PASS" if ok else "FAIL",
                  reason=("golden file differs from contract" if golden != entry["row_count"]
                          else "invalid normalized timestamp" if invalid_rows
                          else "matched" if ok else "row count or content mismatch"))
    if not ok:
        key = identifier(columns[0])
        missing_sample = warehouse.sql(
            cte + f"SELECT m.* FROM missing m LEFT ANTI JOIN extra e ON m.{key} <=> e.{key} LIMIT 5")
        extra_sample = warehouse.sql(
            cte + f"SELECT e.* FROM extra e LEFT ANTI JOIN missing m ON m.{key} <=> e.{key} LIMIT 5")
        column_diffs = " UNION ALL ".join(
            f"SELECT m.{key} AS key, {literal(c)} AS column, "
            f"m.{identifier(c)} AS golden, e.{identifier(c)} AS actual "
            f"FROM missing m JOIN extra e ON m.{key} <=> e.{key} "
            f"WHERE NOT (m.{identifier(c)} <=> e.{identifier(c)})" for c in columns)
        differences = warehouse.sql(cte + f"SELECT * FROM ({column_diffs}) LIMIT 10")
        result["diff"] = dict(missing=missing_sample, extra=extra_sample,
                              columns=[dict(zip(("key", "column", "golden", "actual"), row))
                                       for row in differences])
    return result


def run(client, catalog, suffix, warehouse_id, registry, drop="D2", *,
        contract=None, warehouse=None, schema_override=None):
    if catalog not in ("daiwt_02_mig_dev", "daiwt_02_mig_prod"):
        raise ValueError("Unsupported catalog")
    if catalog.endswith("_dev") and not re.fullmatch(r"pr_[0-9]+", suffix):
        raise ValueError("Dev suffix must match ^pr_[0-9]+$")
    if schema_override and catalog != "daiwt_02_mig_dev":
        raise ValueError("Schema override is dev-only")
    contract = contract if contract is not None else json.loads((ROOT / "src/parity/contract.json").read_text())
    entries = select_entries(contract, drop)
    producers = {}
    for unit, spec in registry.items():
        for table in spec["outputs"]:
            if table in producers:
                raise ValueError(f"Multiple producers for {table}")
            producers[table] = unit
    warehouse = warehouse if warehouse is not None else Warehouse(client, warehouse_id)
    results = []
    for entry in entries:
        row = dict(table=entry["table"], drop=drop, status="SKIP", reason="not yet migrated",
                   golden_rows=None, actual_rows=None, missing_rows=None, extra_rows=None)
        if entry["table"] in producers:
            row["unit"] = producers[entry["table"]]
            try:
                layer = entry["table"].split(".")[0]
                if layer not in ("bronze", "silver", "gold"):
                    raise ValueError("Unsupported output layer")
                schema = schema_override or (layer if catalog.endswith("_prod") else f"{layer}_{suffix}")
                row.update(compare_entry(warehouse, entry, contract["golden_path"], catalog, schema))
            except Exception as exc:
                row.update(status="FAIL", reason=str(exc))
        results.append(row)
    counts = {status: sum(r["status"] == status for r in results) for status in ("PASS", "FAIL", "SKIP")}
    return dict(status="FAIL" if counts["FAIL"] else "PASS", drop=drop, catalog=catalog,
                suffix=suffix, goldens=18, counts=counts, compared=counts["PASS"] + counts["FAIL"],
                results=results)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="daiwt_02_mig_dev")
    parser.add_argument("--suffix", required=True)
    parser.add_argument("--drop", choices=("D0", "D1", "D2"), default="D2")
    parser.add_argument("--warehouse-id", default="6dc4b80538621b7b")
    parser.add_argument("--profile")
    parser.add_argument("--registry", default=str(ROOT / "src/sql/units/registry.json"))
    parser.add_argument("--output", default="out/parity.json")
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    try:
        from databricks.sdk import WorkspaceClient
        registry = json.loads(Path(args.registry).read_text())
        client = WorkspaceClient(profile=args.profile) if any(
            spec["outputs"] for spec in registry.values()) else None
        result = run(client, args.catalog, args.suffix, args.warehouse_id, registry, args.drop)
    except Exception as exc:
        result = dict(status="FAIL", drop=args.drop, catalog=args.catalog, suffix=args.suffix,
                      goldens=0, counts=dict(PASS=0, FAIL=0, SKIP=0), compared=0, results=[], error=str(exc))
    output.write_text(json.dumps(result, indent=2) + "\n")
    c = result["counts"]
    print(f"{result['status']} {args.drop}: compared {result['compared']}/{result['goldens']} "
          f"(PASS {c['PASS']}, FAIL {c['FAIL']}, SKIP {c['SKIP']})")
    if result["compared"] == 0:
        print("no registered outputs compared; not parity evidence")
    if result.get("error"):
        print(result["error"])
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
