"""Render parity evidence; absent or inconsistent evidence fails closed."""
import argparse
import json
from pathlib import Path


def render(result):
    rows = result["results"]
    valid = (result["status"] == "PASS" and not result.get("error")
             and result["goldens"] == len(rows)
             and all(r["status"] in ("PASS", "SKIP") for r in rows)
             and len({r["table"] for r in rows}) == len(rows))
    lines = ["# DAIWT-02 parity", "", f"**{'PASS' if valid else 'FAIL'}** — {len(rows)} goldens",
             "", "| Golden table | Status | Detail |", "| --- | --- | --- |"]
    def cell(value):
        return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")
    for row in rows:
        lines.append("| " + " | ".join(cell(row.get(k, "")) for k in ("table", "status", "reason")) + " |")
    if result.get("error"):
        lines.extend(["", cell(result["error"])])
    return "\n".join(lines) + "\n", valid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("result", nargs="?", default="out/parity.json")
    parser.add_argument("--output", default="attestation.md")
    args = parser.parse_args()
    try:
        body, ok = render(json.loads(Path(args.result).read_text()))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        body, ok = f"# DAIWT-02 parity\n\n**FAIL** — missing or invalid result: {exc}\n", False
    Path(args.output).write_text(body)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
