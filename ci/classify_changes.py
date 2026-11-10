"""Select registered SQL units affected by a change."""
import json
import subprocess
import sys
from pathlib import Path


def classify(registry, paths):
    shared = any(not p.startswith(("src/sql/units/", "tests/", "README", ".github/")) for p in paths)
    return sorted(registry if shared else (
        unit for unit in registry
        if f"src/sql/units/{unit}.sql" in paths or "src/sql/units/registry.json" in paths
    ))


if __name__ == "__main__":
    registry = json.loads(Path("src/sql/units/registry.json").read_text())
    paths = subprocess.check_output(
        ["git", "diff", "--name-only", sys.argv[1], sys.argv[2]], text=True).splitlines()
    print(json.dumps(classify(registry, paths)))
