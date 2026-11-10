"""Generate src/bronze/tables.json from golden/manifest.json and the legacy OLTP DDL."""
import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "src/bronze/tables.json"
COLUMN = re.compile(r"^\s*\[(\w+)\]\s+(AS\s+\(.*|\[sys\]\.\[\w+\]|[A-Z0-9]+(?:\s*\([^)]*\))?)", re.I)


def snake(name):
    return re.sub(r"_+", "_", re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)).lower()


def bronze_name(source_table):
    schema, table = source_table.split(".")
    return f"{snake(schema)}_{snake(table)}"


def sql_type(declared):
    declared = re.sub(r"\s+", "", declared)
    upper = declared.upper()
    if upper.startswith("AS("):
        if "CONVERT([BIT]" in upper:
            return "BIT"
        if "CONVERT([DATETIME2](7)" in upper:
            return "DATETIME2(7)"
        return "NVARCHAR(MAX)"
    if upper.startswith("[SYS]."):
        return upper.split(".")[1].strip("[]")
    return upper


def delta_type(sql):
    if sql in ("INT", "BIGINT", "DATE"):
        return sql
    if sql == "BIT":
        return "BOOLEAN"
    if sql.startswith("DECIMAL"):
        return sql
    if sql.startswith("DATETIME2"):
        return "TIMESTAMP_NTZ"
    if sql.startswith(("NVARCHAR", "VARBINARY", "GEOGRAPHY")):
        return "STRING"
    raise ValueError(f"Unmapped SQL Server type: {sql}")


def ddl_columns(path):
    body = path.read_text(encoding="utf-8-sig").split("\nGO")[0]
    columns = {}
    for line in body.splitlines():
        match = COLUMN.match(line)
        if match:
            columns[match[1]] = sql_type(match[2])
    pk = re.search(r"PRIMARY KEY (?:CLUSTERED|NONCLUSTERED)?\s*\(([^)]*)\)", body)
    keys = re.findall(r"\[(\w+)\]", pk[1]) if pk else []
    return columns, keys


def build(manifest, legacy):
    tables = []
    for path, meta in sorted(manifest["files"].items()):
        if not path.startswith("inputs/D0/") or meta.get("input_kind") != "base":
            continue
        schema, table = meta["source_table"].split(".")
        ddl = legacy / "wwi-ssdt/wwi-ssdt" / schema / "Tables" / f"{table}.sql"
        types, keys = ddl_columns(ddl)
        if keys != meta["primary_key"]:
            raise ValueError(f"PK mismatch for {meta['source_table']}: {keys} vs {meta['primary_key']}")
        columns = []
        for name in meta["columns"]:
            if name not in types:
                raise ValueError(f"Column {name} missing from {ddl}")
            columns.append({"name": name, "sql_type": types[name], "type": delta_type(types[name])})
        tables.append({
            "source_table": meta["source_table"],
            "file": path.split("/", 2)[2],
            "table": bronze_name(meta["source_table"]),
            "key_semantics": meta["key_semantics"],
            "primary_key": meta["primary_key"],
            "columns": columns,
            "ddl": str(ddl.relative_to(legacy)),
        })
    return {"source": "golden/manifest.json + legacy wwi-ssdt DDL", "tables": tables}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--legacy", type=Path, required=True, help="daiwt-02-enterprise-etl checkout")
    args = parser.parse_args()
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    SPEC.write_text(json.dumps(build(manifest, args.legacy), indent=1) + "\n")
