"""Pure Unity Catalog metadata conventions and SQL renderers."""

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEV_CATALOG = "daiwt_02_mig_dev"
AUDIT_COMMENTS = {
    "_drop": "Golden extraction drop identifier.",
    "_file": "Source input file name.",
    "_source_file": "Source input file path.",
    "_ingested_at": "Lakehouse ingestion timestamp.",
    "_op": "Source change operation.",
    "_row_ordinal": "Source row occurrence within the input file.",
    "_failed_rules": "Names of quality rules not satisfied by this row.",
    "_quarantined_at": "Timestamp when the failing row was copied to quarantine.",
}


def identifier(name):
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("identifier must be a nonempty string without NUL")
    return "`" + name.replace("`", "``") + "`"


def relation(name):
    return ".".join(identifier(part) for part in name.split("."))


def literal(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def guard_catalog(catalog, allow_prod=False):
    if catalog != DEV_CATALOG and not allow_prod:
        raise ValueError("governance requires daiwt_02_mig_dev unless --allow-prod is explicit")


def _read(path):
    return json.loads(path.read_text())


def resolve(layer, table):
    """Unknown tables remain governed; quarantine inherits its source metadata."""
    base = table
    while base.endswith(("_quarantine", "_raw")):
        base = base.rsplit("_", 1)[0]
    spec = _read(Path(__file__).with_name("catalog.json"))
    entry = spec.get(layer, {}).get(base, {})
    if layer == "bronze":
        for path in _read(ROOT / "golden/manifest.json")["files"]:
            if not path.startswith("inputs/"):
                continue
            legacy = Path(path).stem
            parts = legacy.split(".")
            if len(parts) != 3:
                continue
            if "_".join(parts[1:]).lower() == base:
                source = parts[-1].removesuffix("_Archive")
                entry = {"legacy_object": legacy.removesuffix("_Archive"),
                         "source_entity": spec["bronze_entities"].get(source, "unclassified"),
                         "comment": f"WWI source landing for {legacy}."}
                break
    tags = {"layer": layer, "source_entity": entry.get("source_entity", "unclassified"),
            "legacy_object": entry.get("legacy_object", "unclassified"),
            "data_owner": entry.get("data_owner", "daiwt-02-data-engineering"),
            "managed_by": "daiwt-02-migration"}
    if table.endswith("_quarantine"):
        tags["quality_role"] = "quarantine"
    return {"comment": entry.get("comment", f"WWI {layer} table {table}."), "tags": tags}


def column_comments(layer, table):
    comments = dict(AUDIT_COMMENTS)
    if layer == "gold":
        contract = _read(ROOT / "src/parity/contract.json")
        native = _read(ROOT / "golden/native_destination_columns.json")
        entry = next((e for e in contract["entries"] if e["table"] == f"gold.{table}"), None)
        if entry:
            mapping = native.get(entry["source"].split(".")[-1].replace(" ", ""), {})
            for column, legacy in zip(entry["columns"], entry["source_columns"]):
                comments[column] = f"Legacy DW column: {mapping.get(legacy, legacy)}"
    return comments


def lakeflow_table_kwargs(layer, table):
    metadata = resolve(layer, table)
    return {"comment": metadata["comment"],
            "table_properties": {f"daiwt.{k}": v for k, v in metadata["tags"].items()}}


def governance_statements(catalog, schema, layer, table, columns, table_type):
    kind = table_type.upper().replace(" ", "_")
    target = ".".join(map(identifier, (catalog, schema, table)))
    metadata = resolve(layer, table)
    statements = [f"COMMENT ON TABLE {target} IS {literal(metadata['comment'])}"]
    if kind == "VIEW":
        return statements
    alter_kind = {"MATERIALIZED_VIEW": "MATERIALIZED VIEW", "STREAMING_TABLE": "STREAMING TABLE"}.get(kind, "TABLE")
    tags = ", ".join(f"{literal(k)} = {literal(v)}" for k, v in metadata["tags"].items())
    statements.append(f"ALTER {alter_kind} {target} SET TAGS ({tags})")
    comments = column_comments(layer, table)
    statements.extend(f"ALTER {alter_kind} {target} ALTER COLUMN {identifier(c)} COMMENT {literal(comments[c])}"
                      for c in columns if c in comments)
    return statements
