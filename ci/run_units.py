"""Run registered job keys in a bundle target."""
import argparse
import json
import re
import subprocess
from pathlib import Path
from drops import drop_params

def run_units(suffix, registry, units=None, target="dev", drop=None, run_urls=None):
    if target not in ("dev", "prod"):
        raise ValueError("invalid target")
    if (target == "dev" and not re.fullmatch(r"pr_[0-9]+", suffix)) or (target == "prod" and suffix != "prod"):
        raise ValueError("invalid suffix for target")
    params = drop_params(json.loads(Path("golden/manifest.json").read_text()), drop)
    urls = []
    for unit in registry if units is None else units:
        command = ["databricks", "bundle", "run", "-t", target, "--var",
                   f"schema_suffix={suffix}", "--params", ",".join(f"{k}={v}" for k, v in params.items()), registry[unit]["job"]]
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                if "Run URL" in line:
                    url = line.strip()
                    urls.append(url)
                    if run_urls:
                        with Path(run_urls).open("a") as output:
                            output.write(url + "\n")
            if process.wait():
                raise subprocess.CalledProcessError(process.returncode, command)
    return urls


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("suffix")
    parser.add_argument("units", nargs="?")
    parser.add_argument("--target", choices=("dev", "prod"), default="dev")
    parser.add_argument("--drop", required=True)
    parser.add_argument("--run-urls")
    args = parser.parse_args()
    units = json.loads(Path(args.units).read_text()) if args.units else None
    registry = json.loads(Path("src/sql/units/registry.json").read_text())
    run_units(args.suffix, registry, units, args.target, args.drop, args.run_urls)
