import csv
import json

import pytest

from parity.build_contract import CONTRACT, GOLDEN, build, render

MANIFEST = json.loads((GOLDEN / "manifest.json").read_text())
CONTRACT_JSON = json.loads(CONTRACT.read_text())
ENTRIES = CONTRACT_JSON["entries"]


def first_rows(path, n):
    with open(GOLDEN / path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [row for _, row in zip(range(n), reader)]


def all_rows(path):
    with open(GOLDEN / path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def test_regenerating_from_golden_is_byte_identical():
    assert render(build(MANIFEST)) == CONTRACT.read_text()


def test_eighteen_outputs_by_three_drops():
    assert [d["name"] for d in CONTRACT_JSON["drops"]] == ["D0", "D1", "D2"]
    outputs = CONTRACT_JSON["outputs"]
    assert len(outputs) == 18
    assert outputs[:16] == [t["table"] for t in MANIFEST["tables"]]
    assert outputs[16:] == ["gold.integration_lineage", "gold.integration_etl_cutoff"]
    assert len(ENTRIES) == 54
    assert {(e["table"], e["drop"]) for e in ENTRIES} == {
        (t, d) for t in outputs for d in ("D0", "D1", "D2")}
    kinds = [e["kind"] for e in ENTRIES if e["drop"] == "D0"]
    assert (kinds.count("dimension"), kinds.count("fact"), kinds.count("report"),
            kinds.count("integration")) == (8, 6, 2, 2)


def test_entries_match_manifest_files():
    for e in ENTRIES:
        assert e["path"].startswith(f"outputs/{e['drop']}/")
        assert e["table"].startswith("gold.")
        spec = MANIFEST["files"][e["path"]]
        assert (e["sha256"], e["row_count"], e["columns"]) == (
            spec["sha256"], spec["row_count"], spec["columns"])
        assert list(e["compare"]) == e["columns"]
    d2 = {e["table"]: e for e in ENTRIES if e["drop"] == "D2"}
    for t in MANIFEST["tables"]:
        assert (d2[t["table"]]["path"], d2[t["table"]]["columns"]) == (t["path"], t["columns"])
    assert CONTRACT_JSON["csv_conventions"] == MANIFEST["csv_conventions"]


def test_only_lineage_wall_clock_columns_are_normalized():
    normalized = {(e["table"], c, rule) for e in ENTRIES for c, rule in e["compare"].items()
                  if rule != "strict"}
    assert {(t, c) for t, c, _ in normalized} == {
        ("gold.integration_lineage", "data_load_started"),
        ("gold.integration_lineage", "data_load_completed")}
    assert {r for _, _, r in normalized} == {"non_null_datetime7", "gte:data_load_started"}
    for e in ENTRIES:
        for column in ("lineage_key", "valid_from", "valid_to"):
            if column in e["compare"]:
                assert e["compare"][column] == "strict"


def test_lineage_keys_are_deterministic_per_drop():
    for index, drop in enumerate(("D0", "D1", "D2")):
        rows = all_rows(f"outputs/{drop}/tables/Integration.Lineage.csv")
        assert [int(r["lineage_key"]) for r in rows] == list(range(1, 13 * (index + 1) + 1))
        latest = [r["table_name"] for r in rows[-13:]]
        assert latest == [r["table_name"] for r in rows[:13]]


def test_no_lineage_foreign_key_is_implied():
    lineage = {r["lineage_key"] for r in all_rows("outputs/D2/tables/Integration.Lineage.csv")}
    assert "0" not in lineage
    for e in ENTRIES:
        assert set(e) == {"table", "drop", "kind", "source", "path", "sha256", "size_bytes",
                          "row_count", "columns", "source_columns", "compare"}
        if e["kind"] == "dimension" and "lineage_key" in e["columns"]:
            assert first_rows(e["path"], 1)[0]["lineage_key"] == "0"


@pytest.fixture
def tiny_golden(tmp_path):
    manifest = {
        "capture_mode": "tsql", "captured_at_utc": "t", "csv_conventions": {"null": "\\N"},
        "drops": [{"name": "D0", "window_start": None, "window_end": None}],
        "tables": [{"table": "gold.dim_x", "path": "outputs/D0/tables/Dimension.X.csv",
                    "columns": ["x_key", "lineage_key"]}],
        "files": {},
    }
    files = {
        "outputs/D0/tables/Dimension.X.csv": (["x_key", "lineage_key"], "Dimension.X"),
        "outputs/D0/tables/Integration.Lineage.csv": (
            ["lineage_key", "data_load_started", "data_load_completed"], "Integration.Lineage"),
        "outputs/D0/tables/Integration.ETL_Cutoff.csv": (
            ["table_name", "cutoff_time"], "Integration.ETL Cutoff"),
    }
    import hashlib
    for path, (columns, source) in files.items():
        body = (",".join(columns) + "\n").encode()
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / path).write_bytes(body)
        manifest["files"][path] = {"columns": columns, "row_count": 0, "size_bytes": len(body),
                                   "sha256": hashlib.sha256(body).hexdigest(),
                                   "source_table": source, "source_columns": columns}
    return tmp_path, manifest


def test_tiny_golden_builds(tiny_golden):
    root, manifest = tiny_golden
    contract = build(manifest, root)
    assert [e["table"] for e in contract["entries"]] == [
        "gold.dim_x", "gold.integration_lineage", "gold.integration_etl_cutoff"]


def test_changed_golden_file_fails(tiny_golden):
    root, manifest = tiny_golden
    (root / "outputs/D0/tables/Dimension.X.csv").write_text("x_key,lineage_key\n1,0\n")
    with pytest.raises(ValueError, match="sha256"):
        build(manifest, root)


def test_header_mismatch_fails(tiny_golden):
    root, manifest = tiny_golden
    manifest["files"]["outputs/D0/tables/Dimension.X.csv"]["columns"] = ["lineage_key", "x_key"]
    with pytest.raises(ValueError, match="header"):
        build(manifest, root)


def test_missing_drop_file_fails(tiny_golden):
    root, manifest = tiny_golden
    del manifest["files"]["outputs/D0/tables/Integration.Lineage.csv"]
    with pytest.raises(ValueError, match="no file entry"):
        build(manifest, root)
