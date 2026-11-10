import importlib.util
import json
import pathlib
import re

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
UNIT = ROOT / "src/sql/units/wwi_city_to_silver"
EOT = "9999-12-31 23:59:59.9999999"


def load(name):
    spec = importlib.util.spec_from_file_location(name, UNIT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


parity = load("parity")


def row(city_id, valid_from, valid_to=EOT, name="A"):
    return (city_id, name, "S", "C", "N", "T", "R", "SR", "POINT (1 2)", "5", valid_from, valid_to)


def test_staged_columns_follow_native_result_set():
    native = json.loads((ROOT / "golden/native_result_sets.json").read_text())
    text = json.dumps(native)
    assert "City" in text
    golden = (ROOT / "golden/outputs/D0/tables/Dimension.City.csv").read_text().splitlines()[0]
    assert golden.split(",")[1:13] == parity.STAGED


def test_datetime2_normalises_manifest_cutoffs():
    assert parity.datetime2("2016-06-14 00:00:00") == "2016-06-14 00:00:00.0000000"
    assert parity.datetime2("2016-06-06 12:31:17.0000000") == "2016-06-06 12:31:17.0000000"


def test_seed_drop_expects_empty_change_set():
    assert parity.compare_drop("D0", None, {"1": row("1", "a")}, [], 0)["status"] == "PASS"
    assert parity.compare_drop("D0", None, {}, [row("1", "a")], 0)["status"] == "FAIL"


def test_delta_drop_equals_new_golden_rows_with_close_off():
    previous = {"1": row("10", "2016-01-01 00:00:00.0000000")}
    current = {"1": row("10", "2016-01-01 00:00:00.0000000", "2016-06-07 00:00:00.0000000"),
               "2": row("10", "2016-06-07 00:00:00.0000000", name="B")}
    staged = [row("10", "2016-06-07 00:00:00.0000000", name="B")]
    assert parity.compare_drop("D1", previous, current, staged, 0)["status"] == "PASS"
    assert parity.compare_drop("D1", previous, current, staged * 2, 0)["status"] == "FAIL"
    assert parity.compare_drop("D1", previous, current, [], 0)["status"] == "FAIL"
    assert parity.compare_drop("D1", previous, current, staged, 1)["status"] == "FAIL"
    unclosed = {"1": previous["1"], "2": current["2"]}
    assert parity.compare_drop("D1", previous, unclosed, staged, 0)["status"] == "FAIL"


def test_duplicate_versions_are_compared_as_multiset():
    previous = {}
    current = {"1": row("10", "t"), "2": row("10", "t")}
    assert parity.compare_drop("D1", previous, current, [row("10", "t")] * 2, 0)["status"] == "PASS"
    assert parity.compare_drop("D1", previous, current, [row("10", "t")], 0)["status"] == "FAIL"


def test_pipeline_expectations_never_drop_and_quarantine_exists():
    sql = (UNIT / "city_changes.sql").read_text()
    assert "city_changes_quarantine" in sql
    assert not re.search(r"ON VIOLATION", sql, re.I)
    for driver in ("application_cities", "application_state_provinces", "application_countries"):
        assert f"${{silver}}.{driver}_history" in sql
    assert "${bronze}.golden_drops" in sql
    staged = sql.split("CREATE OR REFRESH MATERIALIZED VIEW city_changes (")[1].split("FROM staged s")[0]
    aliases = re.findall(r"AS (wwi_city_id|city|state_province|country|continent|sales_territory|"
                         r"region|subregion|location|latest_recorded_population|valid_from|valid_to)\b",
                         staged)
    assert aliases == parity.STAGED


def test_bundle_resources_and_registry_entry():
    resources = yaml.safe_load((ROOT / "resources/wwi_city_to_silver.job.yml").read_text())["resources"]
    pipelines = yaml.safe_load((ROOT / "resources/wwi_city_to_silver.job.yml").read_text())["resources"]
    pipeline = pipelines["pipelines"]["wwi_city_to_silver_pipeline"]
    assert pipeline["serverless"] and pipeline["catalog"] == "${var.catalog}"
    tasks = [t["task_key"] for t in resources["jobs"]["wwi_city_to_silver"]["tasks"]]
    assert tasks == ["silver_pipeline", "silver_parity"]
    params = [p["name"] for p in resources["jobs"]["wwi_city_to_silver"]["parameters"]]
    assert params == ["drop", "last_cutoff", "new_cutoff"]
    entry = json.loads((ROOT / "src/sql/units/registry.json").read_text())["wwi_city_to_silver"]
    assert entry["job"] == "wwi_city_to_silver" and entry["outputs"] == []
    assert entry["owns"] == ["silver.city_changes", "silver.city_changes_quarantine"]
    assert entry["depends_on"] == ["silver_temporal_views"]
    assert list(json.loads((ROOT / "src/sql/units/registry.json").read_text())).index("silver_temporal_views") < list(json.loads((ROOT / "src/sql/units/registry.json").read_text())).index("wwi_city_to_silver")
