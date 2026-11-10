"""Remove only this bundle's PR schemas or registered production outputs."""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from parity.parity_check import Warehouse, identifier

MARKER = "daiwt-02-migration"
REPO = "Cognition-Partner-Workshops-mirror/daiwt-02-lakehouse-migration"


def candidates(schemas, closed):
    # The optional pr_<n>_ prefix matches schemas deployed while presets.name_prefix renamed them.
    for schema in schemas:
        match = re.fullmatch(r"(?:pr_([0-9]+)_)?(bronze|silver|gold)_pr_([0-9]+)", schema.name)
        if (match and match[1] in (None, match[3]) and schema.comment == MARKER
                and (closed is None or int(match[3]) in closed)):
            yield schema.name


def main():
    from databricks.sdk import WorkspaceClient
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true")
    group.add_argument("--prod", action="store_true")
    args = parser.parse_args()
    client = WorkspaceClient()
    warehouse = Warehouse(client, "6dc4b80538621b7b")
    if args.prod:
        registry = json.loads(Path("src/sql/units/registry.json").read_text())
        for table in sorted({table for spec in registry.values() for table in spec["outputs"]}):
            layer, name = table.split(".")
            if layer not in ("bronze", "silver", "gold"):
                raise ValueError("Refusing to touch non-medallion schema")
            full = f"`daiwt_02_mig_prod`.{identifier(layer)}.{identifier(name)}"
            warehouse.sql(f"DROP TABLE IF EXISTS {full}")
        return
    closed = None
    if not args.all:
        pages = json.loads(subprocess.check_output(
            ["gh", "api", "--paginate", "--slurp", f"repos/{REPO}/pulls?state=closed&per_page=100"],
            text=True))
        closed = {pr["number"] for page in pages for pr in page}
    schemas = list(client.schemas.list(catalog_name="daiwt_02_mig_dev"))
    suffixes = sorted({"pr_" + name.rsplit("_", 1)[1] for name in candidates(schemas, closed)})
    for suffix in suffixes:
        subprocess.run(["databricks", "bundle", "destroy", "-t", "dev", "--var",
                        f"schema_suffix={suffix}", "--auto-approve"], check=True)
        # Also remove verified, marked schemas if the deployment state was already removed.
        for name in candidates(schemas, closed):
            if name.endswith("_" + suffix):
                warehouse.sql(f"DROP SCHEMA IF EXISTS `daiwt_02_mig_dev`.{identifier(name)} CASCADE")


if __name__ == "__main__":
    main()
