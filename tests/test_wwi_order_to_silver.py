import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
UNIT = "wwi_order_to_silver"
UNIT_DIR = ROOT / f"src/sql/units/{UNIT}"
PIPELINE_SQL = (UNIT_DIR / "pipeline.sql").read_text()
WINDOW_SQL = (UNIT_DIR / "window.sql").read_text()
PARITY_SQL = (UNIT_DIR / "parity.sql").read_text()
BRONZE_TABLES = ("sales_orders", "sales_order_lines", "warehouse_package_types", "sales_customers")


def snake(name):
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def order_result_set():
    declaration = json.loads((ROOT / "golden/native_result_sets.json").read_text())["Order"]
    names = re.findall(r"^\s+\[?([A-Za-z][A-Za-z ]*?)\]?\s+(?:date|int|nvarchar|decimal|datetime2)", declaration, flags=re.M)
    return [snake(n) for n in names]


def order_changes_select():
    body = PIPELINE_SQL.split("CREATE OR REFRESH MATERIALIZED VIEW order_changes (", 1)[1]
    select = body.split("AS SELECT", 1)[1].split("FROM order_lines_in_window", 1)[0]
    return re.findall(r"\bAS ([a-z_]+),?\s*$", select, flags=re.M)


def test_registry_entry_owns_only_its_silver_tables():
    spec = json.loads((ROOT / "src/sql/units/registry.json").read_text())[UNIT]
    assert spec["job"] == UNIT
    assert spec["outputs"] == []
    assert all(t.startswith("silver.") for t in spec["owns"])
    assert {"silver.order_changes", "silver.order_changes_quarantine"} <= set(spec["owns"])
    assert spec["depends_on"] == ["bronze_landing_replay"]


def test_staged_columns_follow_native_result_set_order():
    columns = [c for c in order_changes_select() if not c.startswith("_")]
    assert columns == order_result_set()


def test_expectations_never_drop_or_fail():
    assert "EXPECT (" in PIPELINE_SQL
    assert "ON VIOLATION" not in PIPELINE_SQL


def test_extract_window_and_totals_match_legacy():
    assert "CASE WHEN l.LastEditedWhen > o.LastEditedWhen THEN l.LastEditedWhen" in PIPELINE_SQL
    assert ") > w.last_cutoff" in PIPELINE_SQL and ") <= w.new_cutoff" in PIPELINE_SQL
    assert "round(CAST(Quantity AS DECIMAL(10,0)) * CAST(UnitPrice AS DECIMAL(18,2)) * CAST(TaxRate AS DECIMAL(18,3)), 0) / 100" in PIPELINE_SQL
    assert "LastEditedWhen_str AS LastEditedWhen" in PIPELINE_SQL


def test_reads_shared_bronze_landing_tables_only():
    assert "read_files" not in PIPELINE_SQL
    assert "MATERIALIZED VIEW ${bronze_fqn}" not in PIPELINE_SQL
    for table in BRONZE_TABLES:
        assert f"${{bronze_fqn}}.{table}" in PIPELINE_SQL
    assert "bronze_replay_log" in WINDOW_SQL


def test_bundle_wiring():
    resources = yaml.safe_load((ROOT / f"resources/{UNIT}.job.yml").read_text())["resources"]
    pipeline = resources["pipelines"][f"{UNIT}_pipeline"]
    assert pipeline["serverless"] is True
    assert pipeline["catalog"] == "${var.catalog}" and pipeline["schema"] == "${var.silver_schema}"
    tasks = {t["task_key"]: t for t in resources["jobs"][UNIT]["tasks"]}
    assert tasks["silver_pipeline"]["depends_on"] == [{"task_key": "extract_window"}]
    assert tasks["parity"]["depends_on"] == [{"task_key": "silver_pipeline"}]
    params = {p["name"]: p["default"] for p in resources["jobs"][UNIT]["parameters"]}
    assert {params[k] for k in ("drop", "last_cutoff", "new_cutoff")} == {
        "${var.drop}", "${var.last_cutoff}", "${var.new_cutoff}"}
    assert "resources/*.yml" in yaml.safe_load((ROOT / "databricks.yml").read_text())["include"]


def test_parity_is_strict_per_drop():
    assert "raise_error" in PARITY_SQL
    assert "count(*) = 1 AND count_if(status = 'PASS') = 1" in PARITY_SQL
    assert "WHERE drop = :drop" in PARITY_SQL
    assert "quarantine_rows = 0" in PARITY_SQL
