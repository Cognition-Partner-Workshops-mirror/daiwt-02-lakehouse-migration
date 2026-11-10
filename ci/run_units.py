"""Run registered job keys in the dev bundle."""
import json
import re
import subprocess
import sys
from pathlib import Path

def run_units(suffix, registry, units=None):
    if not re.fullmatch(r"pr_[0-9]+", suffix):
        raise ValueError("invalid suffix")
    for unit in registry if units is None else units:
        subprocess.run(["databricks", "bundle", "run", "-t", "dev", "--var",
                        f"schema_suffix={suffix}", registry[unit]["job"]], check=True)


if __name__ == "__main__":
    suffix = sys.argv[1]
    units = json.loads(Path(sys.argv[2]).read_text()) if len(sys.argv) > 2 else None
    registry = json.loads(Path("src/sql/units/registry.json").read_text())
    run_units(suffix, registry, units)
