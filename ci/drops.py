"""Resolve replay parameters from the golden manifest."""
import json
import re
import sys
from datetime import datetime
from pathlib import Path

DROPS = ("D0", "D1", "D2")


def cutoff(value):
    if isinstance(value, list):
        values = {cutoff(row["Cutoff Time"]) for row in value if row["Table Name"] != "Date"}
        if len(values) != 1:
            raise ValueError("inconsistent entity cutoffs")
        return values.pop()
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,7})?", value):
        raise ValueError("invalid datetime2 cutoff")
    base, _, fraction = value.partition(".")
    datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
    return base + "." + fraction.ljust(7, "0")


def drop_params(manifest, drop):
    entries = [entry for entry in manifest["drops"] if entry["name"] == drop]
    if drop not in DROPS or len(entries) != 1:
        raise ValueError("unknown drop")
    return {"drop": drop, **{key: cutoff(entries[0][key]) for key in ("last_cutoff", "new_cutoff")}}


if __name__ == "__main__":
    print(json.dumps(drop_params(json.loads(Path("golden/manifest.json").read_text()), sys.argv[1])))
