"""Run registered job keys in a bundle target."""
import argparse
import json
import re
import subprocess
from pathlib import Path

def run_units(suffix, registry, units=None, target="dev"):
    if target not in ("dev", "prod"):
        raise ValueError("invalid target")
    if (target == "dev" and not re.fullmatch(r"pr_[0-9]+", suffix)) or (target == "prod" and suffix != "prod"):
        raise ValueError("invalid suffix for target")
    for unit in registry if units is None else units:
        subprocess.run(["databricks", "bundle", "run", "-t", target, "--var",
                        f"schema_suffix={suffix}", registry[unit]["job"]], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("suffix")
    parser.add_argument("units", nargs="?")
    parser.add_argument("--target", choices=("dev", "prod"), default="dev")
    args = parser.parse_args()
    units = json.loads(Path(args.units).read_text()) if args.units else None
    registry = json.loads(Path("src/sql/units/registry.json").read_text())
    run_units(args.suffix, registry, units, args.target)
