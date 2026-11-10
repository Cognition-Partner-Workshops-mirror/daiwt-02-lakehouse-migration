import json
import re
from pathlib import Path

import pytest

from units.customer.customer_logic import COLUMNS, TABLES, cutoffs, datetime7, extract_sql, table_name

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = json.loads((ROOT / "golden/manifest.json").read_text())


def test_customer_registry():
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    entry = registry["wwi_customer_to_silver"]
    assert entry["job"] == "wwi_customer_to_silver"
    assert entry["outputs"] == []
    assert entry["owns"] == ["silver.customer_changes", "silver.customer_changes_quarantine"]
    assert entry["depends_on"] == ["bronze_landing_replay"]
    platform = json.loads((ROOT / "src/bronze/tables.json").read_text())
    names = {t["source_table"]: t["table"] for t in platform["tables"]}
    assert all(table_name(t) == names[t] for t in TABLES)
    assert all("inputs/D0/WideWorldImporters." + t + ".csv" in MANIFEST["files"] for t in TABLES)


def test_customer_column_order():
    native = json.loads((ROOT / "golden/native_result_sets.json").read_text())["Customer"]
    names = re.findall(r"^\s*(\[[^\]]+\]|\w+)\s+(?:int|nvarchar|datetime2)", native, re.M)
    assert tuple(n.strip("[]").lower().replace(" ", "_") for n in names) == COLUMNS
    destination = json.loads((ROOT / "golden/native_destination_columns.json").read_text())["Customer"]
    assert {n.lower().replace(" ", "_") for n in destination} == set(COLUMNS)


@pytest.mark.parametrize("drop,last,new", [
    ("D0", "2016-06-06 12:31:17", "2016-06-06 12:31:17"),
    ("D1", "2016-06-06 12:31:17", "2016-06-14 00:00:00"),
    ("D2", "2016-06-14 00:00:00", "2016-06-21 00:00:00"),
])
def test_customer_cutoffs(drop, last, new):
    assert cutoffs(MANIFEST, drop) == (datetime7(last), datetime7(new))


@pytest.mark.parametrize("drop", ["", "D3", "d0"])
def test_customer_unknown_drop(drop):
    with pytest.raises(ValueError, match="Unknown drop"):
        cutoffs(MANIFEST, drop)


def test_customer_sql_empty_window_and_precision():
    last, new = cutoffs(MANIFEST, "D0")
    sql = extract_sql("catalog", "bronze", last, new)
    assert f"ValidFrom > '{last}' AND ValidFrom <= '{new}'" in sql
    assert "CAST(c.CustomerID AS INT)" in sql
    assert "AS TIMESTAMP" not in sql
    assert "9999-12-31 23:59:59.9999999" in sql
    assert "WHERE c.buying_group IS NOT NULL" in sql
    assert "SELECT DISTINCT wwi_customer_id, valid_from FROM changes" in sql
    assert sql.count("ValidFrom <> '2020-01-01 00:00:00.0000000'") == 2


@pytest.mark.parametrize("value", ["", "2016-06-06", "2016-06-06 00:00:00.12345678", "'; DROP TABLE t"])
def test_customer_invalid_timestamp(value):
    with pytest.raises(ValueError):
        datetime7(value)


def test_customer_sql_invalid_arguments():
    last, new = cutoffs(MANIFEST, "D1")
    with pytest.raises(ValueError):
        extract_sql("bad;catalog", "bronze", last, new)
    with pytest.raises(ValueError):
        extract_sql("catalog", "bronze", new, last)


def test_customer_seventh_decimal_preserved():
    assert datetime7("2016-06-06 12:31:17.1234567") == "2016-06-06 12:31:17.1234567"


def test_customer_control_window():
    sql = extract_sql("catalog", "bronze")
    assert "`catalog`.`bronze`.golden_drops" in sql
    assert "`catalog`.`bronze`.bronze_replay_log WHERE status = 'PASS'" in sql
    assert "table_name = 'Customer'" in sql
    assert "ValidFrom > (SELECT last_cutoff FROM run_window)" in sql
    assert "ValidFrom <= (SELECT new_cutoff FROM run_window)" in sql
    assert not (ROOT / "src/units/customer/bronze.py").exists()
    assert "ValidFrom_str AS ValidFrom" in sql
    assert "ValidTo_str AS ValidTo" in sql
    assert "sales_customer_categories" in sql
    assert "sales_buying_groups" in sql


def test_customer_initial_load_date_parameter():
    sql = extract_sql("catalog", "bronze", "0001-01-01 00:00:00",
                      "2016-06-06 12:31:17", initial_load_date="2013-01-01")
    assert sql.count("ValidFrom <> '2013-01-01 00:00:00.0000000'") == 2
    assert "2020-01-01" not in sql
    customer_events = sql.split("'customer' kind")[1].split(" UNION ALL ")[0]
    assert "ValidFrom <>" not in customer_events
    parity = (ROOT / "src/units/customer/parity.sql").read_text()
    assert parity.count("ValidFrom <> '2013-01-01 00:00:00.0000000'") == 2


@pytest.mark.parametrize("initial_load_date", ["", "2013-01-01'; DROP TABLE t", "2013/01/01"])
def test_customer_invalid_initial_load_date(initial_load_date):
    with pytest.raises(ValueError):
        extract_sql("catalog", "bronze", initial_load_date=initial_load_date)


def test_customer_parity_is_strict():
    sql = (ROOT / "src/units/customer/parity.sql").read_text()
    assert sql.count("EXCEPT ALL") >= 5
    assert "raise_error" in sql
    assert "baseline_actual) = 402" in sql
    assert "golden_customer) = 403" in sql
