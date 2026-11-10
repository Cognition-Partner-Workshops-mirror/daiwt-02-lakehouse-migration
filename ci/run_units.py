"""Run only selected registry job keys in the dev bundle."""
import json
import re
import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    suffix, path = sys.argv[1:]
    if not re.fullmatch(r"pr_[0-9]+", suffix):
        raise SystemExit("invalid suffix")
    registry = json.loads(Path("src/sql/units/registry.json").read_text())
    for unit in json.loads(Path(path).read_text()):
        subprocess.run(["databricks", "bundle", "run", "-t", "dev", "--var",
                        f"schema_suffix={suffix}", registry[unit]["job"]], check=True)
