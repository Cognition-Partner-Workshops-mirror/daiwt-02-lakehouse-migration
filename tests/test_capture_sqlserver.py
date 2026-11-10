import csv
import datetime as dt
import importlib.util
import io
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from parity.parity_check import GoldenStore, compare, identifier

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("capture_sqlserver", ROOT / "golden/capture_sqlserver.py")
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)


def test_keyset_composite():
    sql, params = c.keyset(["ID", "ValidFrom"], [12, "2016-01-01 00:00:00.1234567"])
    assert sql == "([ID] > %s) OR ([ID] = %s AND [ValidFrom] > %s)"
    assert params == (12, 12, "2016-01-01 00:00:00.1234567")


@pytest.mark.parametrize("kind,expected", [
    ("datetime2", "CONVERT(varchar(27), [x], 121)"),
    ("date", "CONVERT(varchar(10), [x], 23)"),
    ("decimal", "CONVERT(varchar(50), [x])"),
    ("money", "CONVERT(varchar(50), [x])"),
    ("varbinary", "CONVERT(varchar(max), [x], 2)"),
    ("geography", "[x].STAsText()"),
    ("bit", "CONVERT(nvarchar(max), [x])"),
])
def test_sql_server_formatting(kind, expected):
    assert c.format_sql("[x]", kind) == expected


def test_pages_preserve_seven_digit_key(monkeypatch):
    monkeypatch.setattr(c, "primary_key", lambda *_: ["ValidFrom"])
    requests = []
    def query(_, sql, params):
        requests.append((sql, params))
        return ([{"ValidFrom": "2016-01-01 00:00:00.1234567", "__key_0": "2016-01-01 00:00:00.1234567"}]
                if len(requests) == 1 else [])
    monkeypatch.setattr(c, "query", query)
    cols = [{"name": "ValidFrom", "type_name": "datetime2"}]
    assert list(c.table_rows(None, "dbo.History", cols, 1)) == [["2016-01-01 00:00:00.1234567"]]
    assert requests[1][1] == ("2016-01-01 00:00:00.1234567",)
    assert "ORDER BY src.[ValidFrom]" in requests[0][0]


def test_empty_and_missing_table(monkeypatch):
    monkeypatch.setattr(c, "primary_key", lambda *_: ["ID"])
    monkeypatch.setattr(c, "query", lambda *_: [])
    assert list(c.table_rows(None, "dbo.Empty", [{"name": "ID", "type_name": "int"}])) == []
    with pytest.raises(ValueError, match="Missing table"):
        list(c.table_rows(None, "dbo.Missing", []))


def test_no_primary_key_streaming(monkeypatch):
    class Cursor:
        def __init__(self):
            self.calls = 0
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def execute(self, sql):
            assert "ORDER BY src.[Location].STAsText(), src.[Name]" in sql
        def fetchmany(self, n):
            self.calls += 1
            return [{"Location": "POINT (1 2)", "Name": "x"}] if self.calls == 1 else []
    monkeypatch.setattr(c, "primary_key", lambda *_: [])
    con = SimpleNamespace(cursor=Cursor)
    assert list(c.table_rows(con, "dbo.T", [{"name": "Location", "type_name": "geography"},
                                           {"name": "Name", "type_name": "nvarchar"}])) == [["POINT (1 2)", "x"]]


def test_csv_null_unicode_quotes_and_empty(tmp_path):
    m = c.write_csv(tmp_path, "outputs/D0/test.csv", ["a", "b"], [[None, ""], ["é,\"x\"\n", "0"]])
    raw = (tmp_path / "outputs/D0/test.csv").read_bytes()
    assert b"\r\n" not in raw
    assert list(csv.reader(io.StringIO(raw.decode()))) == [["a", "b"], ["\\N", ""], ['é,"x"\n', "0"]]
    assert m["row_count"] == 2
    assert c.write_csv(tmp_path, "empty.csv", ["a"], [])["row_count"] == 0
    with pytest.raises(ValueError, match="Duplicate"):
        c.write_csv(tmp_path, "bad.csv", ["a", "a"], [])
    with pytest.raises(ValueError, match="width"):
        c.write_csv(tmp_path, "bad.csv", ["a"], [[1, 2]])


def capture_fixture(out, drop="D0"):
    files = {}
    for folder, name, metadata in [
        ("tables", "Dimension.Stock_Item", {"database": c.DW, "source_table": "Dimension.Stock Item"}),
        ("tables", "Integration.Lineage", {"database": c.DW, "source_table": "Integration.Lineage"}),
        ("reports", "sale-by-day", {"report": "sale-by-day"}),
    ]:
        path = f"outputs/{drop}/{folder}/{name}.csv"
        files[path] = c.write_csv(out, path, ["id"], [["1"]], **metadata)
    c.write_json(out / f"capture-{drop}.json", {"source": {}, "captured_at_utc": "now",
                 "drop": {"name": drop}, "files": files})


def test_manifest_last_drop_and_offline_parity(tmp_path):
    capture_fixture(tmp_path)
    capture_fixture(tmp_path, "D2")
    manifest = c.manifest(tmp_path)
    assert len(manifest["skipped_branches"]["packages"]) == 205
    assert [x["table"] for x in manifest["tables"]] == ["gold.dim_stock_item", "gold.report_sale_by_day"]
    assert all(x["path"].startswith("outputs/D2/") for x in manifest["tables"])
    client = SimpleNamespace(
        volumes=SimpleNamespace(read=lambda _: None),
        files=SimpleNamespace(download=lambda path: SimpleNamespace(contents=io.BytesIO(
            (tmp_path / path.removeprefix("volume/")).read_bytes()))))
    store = GoldenStore(client, "volume")
    parsed = store.manifest()
    registry = {"capture": {"outputs": [e["table"] for e in parsed["tables"]]}}
    assert compare(parsed, registry, store.rows, lambda _: [["1"]])["status"] == "PASS"
    for e in parsed["tables"]:
        for col in e["columns"]:
            identifier(col)


def test_manifest_empty_and_tampered(tmp_path):
    with pytest.raises(ValueError, match="No completed"):
        c.manifest(tmp_path)
    capture_fixture(tmp_path)
    (tmp_path / "outputs/D0/tables/Dimension.Stock_Item.csv").write_text("id\n2\n")
    with pytest.raises(ValueError, match="changed"):
        c.manifest(tmp_path)


def test_verify_and_errors(tmp_path, monkeypatch):
    capture_fixture(tmp_path)
    c.manifest(tmp_path)
    estate = SimpleNamespace(connect=lambda _: nullcontext(None))
    monkeypatch.setattr(c, "query", lambda *_: [{"n": 1}])
    c.verify(estate, tmp_path)
    monkeypatch.setattr(c, "query", lambda *_: [{"n": 2}])
    with pytest.raises(ValueError, match="Live count"):
        c.verify(estate, tmp_path)
    (tmp_path / "outputs/D0/tables/Dimension.Stock_Item.csv").write_text("id\n2\n")
    with pytest.raises(ValueError, match="SHA256"):
        c.verify(estate, tmp_path)


def test_verify_large_binary_text_field(tmp_path, monkeypatch):
    capture_fixture(tmp_path)
    capture = tmp_path / "capture-D0.json"
    data = json.loads(capture.read_text())
    relative = "inputs/D0/WideWorldImporters.Application.People.csv"
    data["files"][relative] = c.write_csv(tmp_path, relative, ["Photo"], [["AB" * 100_000]])
    c.write_json(capture, data)
    c.manifest(tmp_path)
    monkeypatch.setattr(c, "query", lambda *_: [{"n": 1}])
    csv.field_size_limit(131072)
    c.verify(SimpleNamespace(connect=lambda _: nullcontext(None)), tmp_path)


def test_input_table_discovery_and_missing(monkeypatch):
    def query(_, sql, params=None):
        if "t.history_table_id" in sql:
            return [{"schema_name": "Sales", "name": "Customers", "history_schema": "Sales",
                     "history_name": "Customers_Archive"}]
        if "FROM sys.tables" in sql:
            return [{"schema_name": "Sales", "name": "Customers"}, {"schema_name": "Sales", "name": "Customers_Archive"}]
        if "OBJECT_DEFINITION" in sql:
            return [{"definition": "SELECT * FROM Sales.Customers JOIN Sales.Customers_Archive AS a ON 1=1"}]
        return []
    monkeypatch.setattr(c, "query", query)
    assert c.input_tables(None) == ["Sales.Customers", "Sales.Customers_Archive"]
    monkeypatch.setattr(c, "query", lambda _, sql, params=None:
                        [{"definition": "SELECT * FROM Sales.Missing"}] if "OBJECT_DEFINITION" in sql else [])
    with pytest.raises(ValueError, match="Unresolved"):
        c.input_tables(None)


def test_implicit_temporal_history_is_included(monkeypatch):
    def query(_, sql, params=None):
        if "t.history_table_id" in sql:
            return [{"schema_name": "Warehouse", "name": "Colors", "history_schema": "Warehouse",
                     "history_name": "Colors_Archive"}]
        if "FROM sys.tables" in sql:
            return [{"schema_name": "Warehouse", "name": "Colors"}]
        if "OBJECT_DEFINITION" in sql:
            return [{"definition": "SELECT * FROM Warehouse.Colors FOR SYSTEM_TIME AS OF @Date"}]
        return []
    monkeypatch.setattr(c, "query", query)
    assert c.input_tables(None) == ["Warehouse.Colors", "Warehouse.Colors_Archive"]


def test_advance_sequence_and_error(monkeypatch, tmp_path):
    seen = []
    monkeypatch.setattr(c, "execute", lambda con, sql, params=None: seen.append((sql, params)))
    monkeypatch.setattr(c, "query", lambda *_: [{"n": 1}])
    monkeypatch.setattr(c, "run_state", lambda: (tmp_path / "state.json", {}))
    estate = SimpleNamespace(connect=lambda _: nullcontext(None))
    c.advance(estate, dt.date(2016, 6, 1), dt.date(2016, 6, 2))
    assert "Configuration_Apply" in seen[0][0]
    assert "DailyProcessToCreateHistory" in seen[1][0]
    assert "@AverageNumberOfCustomerOrdersPerDay=60" in seen[1][0]
    assert "@UpdateCustomFields=0" in seen[1][0]
    assert "Configuration_Remove" in seen[2][0]
    assert all("PopulateDataToCurrentDate" not in sql for sql, _ in seen)
    with pytest.raises(ValueError, match="Window"):
        c.advance(estate, dt.date(2016, 6, 2), dt.date(2016, 6, 1))
    def fail(*_):
        raise RuntimeError("simulation failed")
    monkeypatch.setattr(c, "execute", fail)
    with pytest.raises(RuntimeError, match="simulation failed"):
        c.advance(estate, dt.date(2016, 6, 1), dt.date(2016, 6, 2))


def test_etl_order_and_lineage_failure(monkeypatch, tmp_path):
    events = []
    source_names = {"Movement": "Last Modified When", "Purchase": "Is Order Line Finalized"}
    monkeypatch.setattr(c, "result_columns", lambda _, key:
                        [{"name": source_names.get(key, "ID"), "error_number": None}])
    monkeypatch.setattr(c, "columns", lambda *_:
                        [{"name": n} for n in ["ID", "Last Modifed When", "Is Order Finalized"]])
    monkeypatch.setattr(c, "cutoffs", lambda *_: [])
    monkeypatch.setattr(c, "run_state", lambda: (tmp_path / "state.json", {}))
    monkeypatch.setattr(c, "execute", lambda _, sql, params=None: events.append(sql))
    success = True
    def query(_, sql, params=None):
        events.append(sql)
        if "GetLineageKey" in sql:
            return [{"LineageKey": sum("GetLineageKey" in e for e in events)}]
        if "GetLastETL" in sql:
            return [{"CutoffTime": "2016-06-01"}]
        if "INSERT INTO" in sql:
            return [{"n": 1}]
        return [{"Was Successful": success, "Data Load Completed": "now"} for _ in c.ENTITIES]
    monkeypatch.setattr(c, "query", query)
    estate = SimpleNamespace(connect=lambda _: nullcontext(None))
    c.etl(estate, dt.datetime(2016, 6, 2))
    assert "PopulateDateDimensionForYear" in events[0]
    assert "GetLineageKey" in events[1]
    assert events[2] == "DELETE FROM Integration.City_Staging;"
    assert "GetLastETLCutoffTime" in events[3]
    assert "INSERT INTO [WideWorldImportersDW].[Integration].[City_Staging] ([ID]) EXEC" in events[4]
    assert events[5] == "EXEC Integration.MigrateStagedCityData;"
    assert c.CLEAR_SQL["Purchase"] == "DELETE FROM Integration.Order_Staging;"
    assert any("[Movement_Staging] ([Last Modifed When]) EXEC" in e for e in events)
    assert any("[Purchase_Staging] ([Is Order Finalized]) EXEC" in e for e in events)
    assert "GetStockHoldingUpdates];" in next(e for e in events if "INSERT INTO" in e and "StockHolding" in e)
    success = False
    with pytest.raises(RuntimeError, match="lineage"):
        c.etl(estate, dt.datetime(2016, 6, 2))


def test_etl_preflight_fails_before_mutation(monkeypatch):
    monkeypatch.setattr(c, "result_columns", lambda *_: [])
    monkeypatch.setattr(c, "execute", lambda *_: pytest.fail("must not mutate"))
    with pytest.raises(ValueError, match="Cannot describe"):
        c.etl(SimpleNamespace(connect=lambda _: nullcontext(None)), dt.datetime(2016, 6, 2))


def test_native_destination_mappings_and_fallback():
    assert c.destination_columns("Movement", ["Date Key", "Last Modified When"],
                                 {"Date Key", "Last Modifed When"}) == ["Date Key", "Last Modifed When"]
    assert c.destination_columns("Purchase", ["Is Order Line Finalized"],
                                 {"Is Order Finalized"}) == ["Is Order Finalized"]
    assert c.destination_columns("City", ["Future Column"], {"Future Column"}) == ["Future Column"]
    assert c.destination_columns("City", [], set()) == []
    with pytest.raises(ValueError, match="columns differ"):
        c.destination_columns("Movement", ["Last Modified When"], {"Last Modified When"})
    with pytest.raises(ValueError, match="columns differ"):
        c.destination_columns("Movement", ["Last Modified When", "Last Modifed When"], {"Last Modifed When"})


@pytest.fixture
def delta_root(tmp_path, monkeypatch):
    monkeypatch.setattr(c, "FULL_ROOT", tmp_path / "scratch/full")
    return tmp_path / "golden"


def input_drop(out, drop, rows, previous=None, keys=None, table="dbo.Example", headers=None):
    return c.capture_input(out, drop, table, headers or ["ID", "Value"],
                           ["ID"] if keys is None else keys, rows, previous)


def main_file(files):
    return next((p, m) for p, m in files.items() if m["input_kind"] != "delete")


def test_delta_insert_update_delete_and_reconstruction(delta_root):
    d0 = input_drop(delta_root, "D0", [["1", "old"], ["2", "gone"], ["10", "keep"]])
    p0, m0 = main_file(d0)
    assert c.sha256(delta_root / p0) == m0["full_sha256"]
    d1 = input_drop(delta_root, "D1", [["1", 'new,\"é\"\n'], ["3", "\\N"], ["10", "keep"]], (p0, m0))
    p1, m1 = main_file(d1)
    assert list(c.csv_rows(delta_root / p1, ["ID", "Value"])) == [["1", 'new,\"é\"\n'], ["3", "\\N"]]
    deleted = next((p, m) for p, m in d1.items() if m["input_kind"] == "delete")
    assert deleted[0].endswith(".deleted.csv")
    assert deleted[1]["columns"] == ["ID"]
    assert list(c.csv_rows(delta_root / deleted[0], ["ID"])) == [["2"]]
    assert m1["row_count"] == 2 and m1["full_row_count"] == 3
    assert m1["sha256"] != m1["full_sha256"]
    d2 = input_drop(delta_root, "D2", [["1", 'new,\"é\"\n'], ["3", "\\N"], ["10", "keep"]], (p1, m1))
    p2, m2 = main_file(d2)
    assert m2["row_count"] == 0 and len(d2) == 1
    assert (delta_root / p2).read_bytes() == b"ID,Value\n"
    c.verify_reconstruction(delta_root, {**d0, **d1, **d2})


def test_empty_inputs_and_all_deleted(delta_root):
    d0 = input_drop(delta_root, "D0", [])
    d1 = input_drop(delta_root, "D1", [["2", ""]], main_file(d0))
    d2 = input_drop(delta_root, "D2", [], main_file(d1))
    assert main_file(d2)[1]["row_count"] == 0
    assert main_file(d2)[1]["full_row_count"] == 0
    assert next(m for m in d2.values() if m["input_kind"] == "delete")["row_count"] == 1
    c.verify_reconstruction(delta_root, {**d0, **d1, **d2})


def test_no_pk_multiset_delta(delta_root):
    d0 = input_drop(delta_root, "D0", [["a", "old"], ["a", "old"], ["b", "same"]], keys=[])
    d1 = input_drop(delta_root, "D1", [["a", "old"], ["b", "same"], ["b", "same"], ["c", "new"]],
                    main_file(d0), keys=[])
    p1, m1 = main_file(d1)
    assert m1["key_semantics"] == "whole_row_multiset"
    assert list(c.csv_rows(delta_root / p1, ["ID", "Value"])) == [["b", "same"], ["c", "new"]]
    deletion = next((p, m) for p, m in d1.items() if m["input_kind"] == "delete")
    assert deletion[1]["columns"] == ["ID", "Value"]
    assert list(c.csv_rows(delta_root / deletion[0], ["ID", "Value"])) == [["a", "old"]]
    c.verify_reconstruction(delta_root, {**d0, **d1})


def test_composite_pk_delta(delta_root):
    headers = ["ID", "ValidFrom", "Value"]
    keys = ["ID", "ValidFrom"]
    d0 = input_drop(delta_root, "D0", [["1", "2016-01-01 00:00:00.1234567", "old"]], keys=keys, headers=headers)
    d1 = input_drop(delta_root, "D1", [["1", "2016-01-01 00:00:00.1234567", "new"],
                                      ["1", "2016-01-01 00:00:00.1234568", "other"]],
                    main_file(d0), keys=keys, headers=headers)
    assert main_file(d1)[1]["row_count"] == 2
    c.verify_reconstruction(delta_root, {**d0, **d1})


def test_delta_requires_unchanged_previous_full(delta_root):
    with pytest.raises(ValueError, match="No previous"):
        input_drop(delta_root, "D1", [])
    d0 = input_drop(delta_root, "D0", [["1", "old"]])
    p0, m0 = main_file(d0)
    with pytest.raises(ValueError, match="schema/primary key changed"):
        input_drop(delta_root, "D1", [], (p0, m0), keys=["Value"])
    Path(m0["full_path"]).write_text("ID,Value\n1,changed\n")
    with pytest.raises(ValueError, match="missing or changed"):
        input_drop(delta_root, "D1", [], (p0, m0))


def test_duplicate_pk_rejected(delta_root):
    d0 = input_drop(delta_root, "D0", [["1", "old"]])
    with pytest.raises(ValueError, match="Duplicate primary key"):
        input_drop(delta_root, "D1", [["1", "a"], ["1", "b"]], main_file(d0))


def test_reconstruction_detects_changed_scratch_and_deltas(delta_root):
    d0 = input_drop(delta_root, "D0", [["1", "old"]])
    d1 = input_drop(delta_root, "D1", [["1", "new"]], main_file(d0))
    p1, m1 = main_file(d1)
    (delta_root / p1).write_text("ID,Value\n1,wrong\n")
    with pytest.raises(ValueError, match="Reconstruction content"):
        c.verify_reconstruction(delta_root, {**d0, **d1})
    Path(m1["full_path"]).write_text("ID,Value\n1,changed\n")
    with pytest.raises(ValueError, match="Full scratch SHA256"):
        c.verify_reconstruction(delta_root, {p1: m1})


def test_missing_scratch_is_explicit_skip(delta_root, capsys):
    d0 = input_drop(delta_root, "D0", [["1", "old"]])
    Path(main_file(d0)[1]["full_path"]).unlink()
    c.verify_reconstruction(delta_root, d0)
    assert "RECONSTRUCT SKIP" in capsys.readouterr().out


def test_reconstruction_deletion_and_cycle_errors(delta_root):
    d0 = input_drop(delta_root, "D0", [["1", "old"]])
    d1 = input_drop(delta_root, "D1", [], main_file(d0))
    p1, m1 = main_file(d1)
    deletion = next(p for p, m in d1.items() if m["input_kind"] == "delete")
    (delta_root / deletion).write_text("ID\n2\n")
    with pytest.raises(ValueError, match="Deleted key absent"):
        c.verify_reconstruction(delta_root, {**d0, **d1})
    m1["previous_input"] = p1
    with pytest.raises(ValueError, match="Cycle"):
        c.verify_reconstruction(delta_root, {**d0, **d1})


def test_manifest_numeric_drop_order_and_semantics(tmp_path):
    for drop in ["D0", "D2", "D10"]:
        capture_fixture(tmp_path, drop)
    manifest = c.manifest(tmp_path)
    assert [d["name"] for d in manifest["drops"]] == ["D0", "D2", "D10"]
    assert manifest["tables"][0]["path"].startswith("outputs/D10/")
    assert manifest["input_semantics"]["D0"] == "full base"
    with pytest.raises(ValueError, match="Invalid drop"):
        c.drop_number("D00")


def test_delta_manifest_and_verify_offline(delta_root, monkeypatch, capsys):
    d0 = input_drop(delta_root, "D0", [["1", "base"], ["2", "gone"]])
    d1 = input_drop(delta_root, "D1", [["1", "changed"], ["3", "inserted"]], main_file(d0))
    for drop, files in [("D0", d0), ("D1", d1)]:
        capture_fixture(delta_root, drop)
        path = delta_root / f"capture-{drop}.json"
        capture = json.loads(path.read_text())
        capture["files"].update(files)
        c.write_json(path, capture)
    manifest = c.manifest(delta_root)
    assert manifest["files"][main_file(d1)[0]]["full_row_count"] == 2
    monkeypatch.setattr(c, "query", lambda *_: [{"n": 1}])
    c.verify(SimpleNamespace(connect=lambda _: nullcontext(None)), delta_root)
    output = capsys.readouterr().out
    assert output.count("RECONSTRUCT PASS") == 2
    assert "VERIFY PASS" in output


def test_split_export_inputs_before_outputs(delta_root, monkeypatch):
    monkeypatch.setattr(c, "legacy_commit", lambda: "legacy-sha")
    monkeypatch.setattr(c, "input_tables", lambda _: ["dbo.Example"])
    monkeypatch.setattr(c, "columns", lambda *_: [{"name": "ID", "type_name": "int"}])
    monkeypatch.setattr(c, "primary_key", lambda *_: ["ID"])
    monkeypatch.setattr(c, "table_rows", lambda *_: iter([["1"]]))
    monkeypatch.setattr(c, "REPORTS", {})
    current = "2016-06-06 12:31:17"
    monkeypatch.setattr(c, "cutoffs", lambda _: [{"Table Name": "Example", "Cutoff Time": current}])
    state = {}
    monkeypatch.setattr(c, "run_state", lambda: (delta_root / "state.json", state))
    monkeypatch.setattr(c, "query", lambda _, sql, params=None:
                        [{"version": "SQL Server", "edition": "Express"}] if "@@VERSION" in sql
                        else [{"table_name": "Dimension.Example"}])
    estate = SimpleNamespace(secret={"host": "example"}, connect=lambda _: nullcontext(None))
    c.export(estate, "D0", delta_root)
    baseline = json.loads((delta_root / "capture-D0.json").read_text())
    assert baseline["drop"]["window_start"] is None
    state["advance"] = {"start": "2016-06-07", "end": "2016-06-13"}
    c.export(estate, "D1", delta_root, "inputs")
    assert not (delta_root / "capture-D1.json").exists()
    assert not (delta_root / "outputs/D1").exists()
    assert (delta_root / ".capture-D1-inputs.json").exists()
    current = "2016-06-14 00:00:00"
    state["etl"] = {"new_cutoff": current, "last_cutoff": baseline["drop"]["new_cutoff"]}
    c.export(estate, "D1", delta_root, "outputs")
    completed = json.loads((delta_root / "capture-D1.json").read_text())
    assert completed["drop"]["window_start"] == "2016-06-07"
    assert completed["drop"]["new_cutoff"] == current
    assert len(completed["files"]) == 2
    assert next(m for p, m in completed["files"].items() if p.startswith("inputs/"))["row_count"] == 0
    with pytest.raises(ValueError, match="Export inputs before outputs"):
        c.export(estate, "D2", delta_root, "outputs")


def test_advance_zero_activity_stops(monkeypatch, tmp_path):
    monkeypatch.setattr(c, "execute", lambda *_: None)
    monkeypatch.setattr(c, "query", lambda *_: [{"n": 0}])
    monkeypatch.setattr(c, "run_state", lambda: pytest.fail("must not record success"))
    with pytest.raises(RuntimeError, match="zero OLTP activity"):
        c.advance(SimpleNamespace(connect=lambda _: nullcontext(None)), dt.date(2016, 6, 7), dt.date(2016, 6, 13))
