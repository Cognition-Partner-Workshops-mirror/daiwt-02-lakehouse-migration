import json
from pathlib import Path
import sys
import types

import pytest
import yaml

from governance import (DEV_CATALOG, column_comments, governance_statements,
                        guard_catalog, identifier, lakeflow_table_kwargs,
                        literal, resolve)
from governance.apply_governance import apply_governance
from governance.quality import (failed_rules_expr, quarantine_frame,
                                quarantine_sql, warn_expectations)
from governance.smoke import statements


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("layer,table,entity,legacy", [
    ("bronze", "sales_customers", "customer", "WideWorldImporters.Sales.Customers"),
    ("bronze", "sales_customers_archive_raw", "customer", "WideWorldImporters.Sales.Customers"),
    ("bronze", "application_payment_methods_raw", "payment_method", "WideWorldImporters.Application.PaymentMethods"),
    ("bronze", "warehouse_stock_item_holdings", "stock_holding", "WideWorldImporters.Warehouse.StockItemHoldings"),
    ("silver", "customer_changes", "customer", "Integration.GetCustomerUpdates"),
    ("silver", "stock_holding_snapshot", "stock_holding", "Integration.GetStockHoldingUpdates"),
    ("gold", "dim_customer", "customer", "Dimension.Customer"),
    ("gold", "fact_sale_raw", "sale", "Fact.Sale"),
])
def test_resolve(layer, table, entity, legacy):
    metadata = resolve(layer, table)
    assert metadata["comment"]
    assert metadata["tags"] == {
        "layer": layer, "source_entity": entity, "legacy_object": legacy,
        "data_owner": "daiwt-02-data-engineering", "managed_by": "daiwt-02-migration",
    }


def test_quarantine_and_unknown():
    tags = resolve("silver", "customer_changes_quarantine")["tags"]
    assert tags["source_entity"] == "customer"
    assert tags["quality_role"] == "quarantine"
    for layer in ("bronze", "silver", "gold"):
        unknown = resolve(layer, "new_table")
        assert unknown["comment"]
        assert unknown["tags"]["source_entity"] == "unclassified"
        assert unknown["tags"]["layer"] == layer
        assert unknown["tags"]["managed_by"] == "daiwt-02-migration"


def test_manifest_and_contract_coverage():
    manifest = json.loads((ROOT / "golden/manifest.json").read_text())
    for path in manifest["files"]:
        if path.startswith("inputs/"):
            table = "_".join(Path(path).stem.split(".")[1:]).lower()
            assert resolve("bronze", table)["tags"]["source_entity"] != "unclassified"
    contract = json.loads((ROOT / "src/parity/contract.json").read_text())
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    for table in registry["bronze_landing_replay"]["owns"]:
        layer, name = table.split(".")
        if name not in {"bronze_replay_log", "golden_drops"}:
            assert resolve(layer, name)["tags"]["source_entity"] != "unclassified"
    for table in contract["outputs"]:
        assert resolve(*table.split("."))["tags"]["source_entity"] != "unclassified"


def test_columns_and_rendering():
    comments = column_comments("gold", "dim_customer")
    assert comments["customer_key"] == "Legacy DW column: Customer Key"
    sql = governance_statements("cat", "gold_pr_1", "gold", "dim_customer",
                                ["customer_key", "_drop", "unknown"], "MANAGED")
    assert sql[0] == "COMMENT ON TABLE `cat`.`gold_pr_1`.`dim_customer` IS 'WWI customer dimension.'"
    assert "SET TAGS ('layer' = 'gold'" in sql[1]
    assert "ALTER COLUMN `customer_key` COMMENT 'Legacy DW column: Customer Key'" in sql[2]
    assert len(sql) == 4
    assert "OWNER TO" not in "\n".join(sql)
    assert len(governance_statements("c", "s", "silver", "t", [], "MANAGED")) == 2


@pytest.mark.parametrize("kind,alter", [("MATERIALIZED_VIEW", "MATERIALIZED VIEW"),
                                         ("STREAMING_TABLE", "STREAMING TABLE")])
def test_pipeline_objects(kind, alter):
    sql = governance_statements("c", "s", "silver", "customer_changes", ["_drop"], kind)
    assert sql[0].startswith("COMMENT ON TABLE")
    assert sql[1].startswith(f"ALTER {alter}")
    assert sql[2].startswith(f"ALTER {alter}")
    kwargs = lakeflow_table_kwargs("silver", "customer_changes_quarantine")
    assert kwargs["comment"]
    assert kwargs["table_properties"]["daiwt.quality_role"] == "quarantine"
    assert len(governance_statements("c", "s", "gold", "v", ["_drop"], "VIEW")) == 1


def test_escaping():
    assert identifier("a`b") == "`a``b`"
    assert literal("a'b\\c") == r"'a\'b\\c'"
    sql = governance_statements("a`b", "s", "silver", "t'\\x", ["_drop"], "MANAGED")
    assert "`a``b`" in sql[0]
    assert r"t\'\\x" in sql[0]
    with pytest.raises(ValueError):
        identifier("")
    with pytest.raises(ValueError):
        identifier("a\x00b")


def test_guard_before_execution():
    guard_catalog(DEV_CATALOG)
    guard_catalog("daiwt_02_mig_prod", allow_prod=True)
    for catalog in ("daiwt_02_mig_prod", "other"):
        with pytest.raises(ValueError, match="--allow-prod"):
            apply_governance(catalog, ["gold"], lambda _: pytest.fail("must not execute"))
    with pytest.raises(ValueError, match="medallion"):
        apply_governance(DEV_CATALOG, ["gold_pr_1", "landing"], lambda _: pytest.fail("must not execute"))


def test_spark_python_task_bootstrap_without_file(monkeypatch):
    script = ROOT / "src/governance/apply_governance.py"
    monkeypatch.setattr(sys, "argv", [str(script)])
    monkeypatch.setattr(sys, "path", list(sys.path))
    namespace = {"__name__": "spark_task_bootstrap", "__package__": None}
    exec(compile(script.read_text(), str(script), "exec"), namespace)
    assert sys.path[0] == str(ROOT / "src")
    assert callable(namespace["main"])


def test_apply_discovery_and_summary(capsys):
    seen = []

    def execute(sql):
        seen.append(sql)
        if ".tables WHERE" in sql:
            return [{"table_name": "dim_customer", "table_type": "MANAGED"}]
        if ".columns WHERE" in sql:
            return [{"table_name": "dim_customer", "column_name": "customer_key"}]
        return []

    assert apply_governance(DEV_CATALOG, ["gold_pr_1"], execute) == (1, 0)
    assert any("Customer Key" in sql for sql in seen)
    assert "Governance: 1 tables governed / 0 skipped" in capsys.readouterr().out
    assert apply_governance(DEV_CATALOG, ["gold_pr_1"], lambda _: []) == (0, 0)


def test_quality_sql():
    rules = {"positive'key": "id > 0", "named": "name IS NOT NULL"}
    expr = failed_rules_expr(rules)
    assert "coalesce((id > 0), false)" in expr
    assert r"'positive\'key'" in expr
    assert "rule -> rule IS NOT NULL" in expr
    assert failed_rules_expr({}) == "CAST(array() AS ARRAY<STRING>)"
    assert warn_expectations({}) == {}
    assert warn_expectations(rules)["named"] == "coalesce((name IS NOT NULL), false)"
    sql = quarantine_sql("c.s.source", "c.s.source_quarantine", rules, "D'0")
    assert sql[0].startswith("CREATE TABLE IF NOT EXISTS `c`.`s`.`source_quarantine`")
    assert "WHERE false" in sql[0]
    assert sql[1] == r"DELETE FROM `c`.`s`.`source_quarantine` q WHERE q._drop <=> 'D\'0'"
    assert r"s.`_drop` <=> 'D\'0'" in sql[2]
    assert "size(filter(" in sql[2]
    assert "s.* EXCEPT (_drop)" in sql[2]
    for statement in sql:
        assert not statement.startswith(("DELETE FROM `c`.`s`.`source`", "INSERT INTO `c`.`s`.`source`", "UPDATE "))
    assert "EXISTS (SELECT 1 FROM `c`.`s`.`source`" in quarantine_sql("c.s.source", "c.s.q", {}, None)[1]
    with pytest.raises(ValueError):
        quarantine_sql("c.s.source", "C.S.SOURCE", rules)


def test_lazy_frame_helper(monkeypatch):
    class Frame:
        def withColumn(self, name, expr):
            assert name == "_failed_rules"
            assert expr == failed_rules_expr({})
            return self

        def where(self, predicate):
            assert predicate
            return self

    functions = types.SimpleNamespace(expr=lambda expr: expr, size=lambda name: 1)
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", types.SimpleNamespace(functions=functions))
    frame = Frame()
    assert quarantine_frame(frame, {}) is frame


def test_job_and_registry():
    job = yaml.safe_load((ROOT / "resources/governance.job.yml").read_text())["resources"]["jobs"]["governance"]
    assert job["name"] == "${var.job_name_prefix}governance"
    assert {p["name"]: p["default"] for p in job["parameters"]} == {
        k: "${var." + k + "}" for k in ("drop", "last_cutoff", "new_cutoff")}
    assert job["environments"][0]["spec"]["environment_version"] == "2"
    task = job["tasks"][0]
    assert task["environment_key"] == job["environments"][0]["environment_key"]
    assert task["spark_python_task"]["python_file"] == "../src/governance/apply_governance.py"
    assert "--allow-prod" not in task["spark_python_task"]["parameters"]
    registry = json.loads((ROOT / "src/sql/units/registry.json").read_text())
    assert registry["governance"] == {"job": "governance", "outputs": [], "owns": [], "depends_on": []}
    assert yaml.safe_load((ROOT / "databricks.yml").read_text())["include"] == ["resources/*.yml"]


def test_smoke_is_dev_scoped_and_table_only_cleanup():
    sql = list(statements("pr_123"))
    assert sum(s.startswith("DROP TABLE ") for s in sql) == 4
    assert not any("DROP SCHEMA" in s or "CREATE OR REPLACE" in s for s in sql)
    assert not any("prod" in s for s in sql)
    assert any("column_tags" in s for s in sql)
    assert all("schema_name = '" in s and "table_schema" not in s
               for s in sql if ".table_tags " in s or ".column_tags " in s)
    retained = list(statements("pr_123", keep=True))
    assert retained == sql[:-4]
    assert not any(s.startswith("DROP ") for s in retained)
    with pytest.raises(ValueError):
        list(statements("prod"))
