"""Read-only dev parity diagnostics; preserve the full multiset differences."""
import argparse
import json
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from parity.parity_check import Warehouse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("drop", choices=("D0", "D1", "D2"))
    args = parser.parse_args()
    params = {"catalog": "daiwt_02_mig_dev", "bronze_schema": "bronze_pr_7",
              "silver_schema": "silver_pr_7", "drop": args.drop,
              "golden_path": "/Volumes/daiwt_02_mig_prod/landing/golden"}
    sql = Path(__file__).with_name("parity.sql").read_text().split("SELECT CASE WHEN :drop")[0].rstrip()
    for key, value in params.items():
        sql = sql.replace(":" + key, "'" + value + "'")
    sql += """,
actual_only AS (SELECT * FROM baseline_actual EXCEPT ALL SELECT * FROM baseline_expected),
expected_only AS (SELECT * FROM baseline_expected EXCEPT ALL SELECT * FROM baseline_actual)
SELECT 'metrics' kind, to_json(named_struct(
 'staged_rows', (SELECT count(*) FROM actual_changes),
 'golden_gain_rows', (SELECT count(*) FROM expected_changes),
 'staged_extra', (SELECT count(*) FROM (SELECT * FROM actual_changes EXCEPT ALL SELECT * FROM expected_changes)),
 'staged_missing', (SELECT count(*) FROM (SELECT * FROM expected_changes EXCEPT ALL SELECT * FROM actual_changes)),
 'golden_rows', (SELECT count(*) FROM golden_customer),
 'golden_key_zero_rows', (SELECT count(*) FROM golden_customer WHERE customer_key = 0),
 'quarantine_rows', (SELECT count(*) FROM daiwt_02_mig_dev.silver_pr_7.customer_changes_quarantine),
 'baseline_actual_rows', (SELECT count(*) FROM baseline_actual),
 'baseline_expected_rows', (SELECT count(*) FROM baseline_expected),
 'baseline_extra', (SELECT count(*) FROM actual_only),
 'baseline_missing', (SELECT count(*) FROM expected_only))) body
UNION ALL SELECT 'baseline_extra', to_json(struct(*)) FROM actual_only
UNION ALL SELECT 'baseline_missing', to_json(struct(*)) FROM expected_only
"""
    rows = Warehouse(WorkspaceClient(), "6dc4b80538621b7b").sql(sql)
    report = {"drop": args.drop, "metrics": {}, "baseline_extra": [], "baseline_missing": []}
    for kind, body in rows:
        value = json.loads(body)
        if kind == "metrics":
            report["metrics"] = value
        else:
            report[kind].append(value)
    path = Path("out") / f"customer_{args.drop}_diagnostics.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"drop": args.drop, **report["metrics"]}, indent=2))
    for key in ("baseline_extra", "baseline_missing"):
        print(key, "sample:", json.dumps(report[key][:3], indent=2))
    print("Full diff:", path)


if __name__ == "__main__":
    main()
