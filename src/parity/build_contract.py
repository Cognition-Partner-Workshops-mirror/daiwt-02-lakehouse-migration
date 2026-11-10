"""Derive the per-drop parity contract from the golden manifest (golden is read-only)."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "golden"
CONTRACT = ROOT / "src/parity/contract.json"
GOLDEN_VOLUME = "/Volumes/daiwt_02_mig_prod/landing/golden"
INTEGRATION_TABLES = {
    "gold.integration_lineage": "Integration.Lineage.csv",
    "gold.integration_etl_cutoff": "Integration.ETL_Cutoff.csv",
}
NORMALIZE = {
    "gold.integration_lineage": {
        "data_load_started": "non_null_datetime7",
        "data_load_completed": "gte:data_load_started",
    },
}
KINDS = {"Dimension": "dimension", "Fact": "fact", "Integration": "integration"}


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def outputs(manifest):
    """(gold table, path relative to a drop's outputs dir) for the 18 outputs."""
    result = []
    for entry in manifest["tables"]:
        prefix, drop, rest = entry["path"].split("/", 2)
        if prefix != "outputs":
            raise ValueError(f"Unexpected golden path: {entry['path']}")
        result.append((entry["table"], rest))
    result += [(table, f"tables/{name}") for table, name in INTEGRATION_TABLES.items()]
    if len({t for t, _ in result}) != len(result):
        raise ValueError("Duplicate output table")
    return result


def kind(relative):
    folder, name = relative.split("/", 1)
    return "report" if folder == "reports" else KINDS[name.split(".", 1)[0]]


def build(manifest, golden_dir=GOLDEN, verify_files=True):
    drops = [d["name"] for d in manifest["drops"]]
    entries = []
    for drop in drops:
        for table, relative in outputs(manifest):
            path = f"outputs/{drop}/{relative}"
            spec = manifest["files"].get(path)
            if spec is None:
                raise ValueError(f"Manifest has no file entry for {path}")
            columns = spec["columns"]
            normalize = NORMALIZE.get(table, {})
            if not set(normalize) <= set(columns):
                raise ValueError(f"Normalized column missing from {path}")
            if verify_files:
                local = Path(golden_dir) / path
                with open(local, encoding="utf-8", newline="") as f:
                    header = f.readline().rstrip("\n").split(",")
                if header != columns:
                    raise ValueError(f"CSV header differs from manifest columns: {path}")
                if sha256(local) != spec["sha256"]:
                    raise ValueError(f"sha256 differs from manifest: {path}")
            entries.append({
                "table": table,
                "drop": drop,
                "kind": kind(relative),
                "source": spec.get("source_table") or spec["report"],
                "path": path,
                "sha256": spec["sha256"],
                "size_bytes": spec["size_bytes"],
                "row_count": spec["row_count"],
                "columns": columns,
                "source_columns": spec["source_columns"],
                "compare": {c: normalize.get(c, "strict") for c in columns},
            })
    return {
        "generated_from": "golden/manifest.json",
        "golden_path": GOLDEN_VOLUME,
        "capture_mode": manifest["capture_mode"],
        "captured_at_utc": manifest["captured_at_utc"],
        "csv_conventions": manifest["csv_conventions"],
        "drops": [{k: d[k] for k in ("name", "window_start", "window_end")} for d in manifest["drops"]],
        "outputs": [t for t, _ in outputs(manifest)],
        "entries": entries,
    }


def render(contract):
    return json.dumps(contract, indent=2, ensure_ascii=False) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="fail if the committed contract is stale")
    args = parser.parse_args()
    text = render(build(json.loads((GOLDEN / "manifest.json").read_text())))
    if args.check:
        if CONTRACT.read_text() != text:
            print("contract.json is stale; run python src/parity/build_contract.py")
            return 1
        print("contract.json up to date")
        return 0
    CONTRACT.write_text(text)
    print(f"wrote {CONTRACT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
