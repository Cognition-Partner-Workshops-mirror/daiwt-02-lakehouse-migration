"""Select registered SQL units affected by a change."""
import json
import subprocess
import sys
from registry import load, topo_order


def expand(registry, units):
    if units == "*" or "*" in units:
        return topo_order(registry, registry)
    selected = set(units)
    if selected - registry.keys():
        raise ValueError("unknown units: " + ", ".join(sorted(selected - registry.keys())))
    while True:
        dependants = {unit for unit, spec in registry.items() if selected.intersection(spec.get("depends_on", []))}
        if dependants <= selected:
            return topo_order(registry, selected)
        selected |= dependants


def classify(registry, paths, base_registry=None):
    selected = set()
    registry_selected = set()
    for path in paths:
        if path.startswith(("tests/", "README", "docs/", ".github/")):
            continue
        if path == "src/sql/units/registry.json":
            if base_registry is None:
                raise ValueError("base registry required for registry changes")
            registry_selected |= {unit for unit, spec in registry.items()
                                  if unit not in base_registry or spec != base_registry[unit]}
            continue
        matches = {unit for unit in registry if path in (f"src/sql/units/{unit}.sql", f"resources/{unit}.job.yml")
                   or path.startswith(f"src/sql/units/{unit}/")}
        if not matches:
            return topo_order(registry, registry)
        selected |= matches
    while True:
        dependencies = {dependency for unit in registry_selected
                        for dependency in registry[unit].get("depends_on", [])}
        if dependencies <= registry_selected:
            return topo_order(registry, set(expand(registry, selected)) | registry_selected)
        registry_selected |= dependencies


def classify_diff(registry, base, head):
    paths = subprocess.check_output(
        ["git", "diff", "--name-only", base, head], text=True).splitlines()
    base_registry = None
    if "src/sql/units/registry.json" in paths:
        base_registry = json.loads(subprocess.check_output(
            ["git", "show", f"{base}:src/sql/units/registry.json"], text=True))
    return classify(registry, paths, base_registry)


if __name__ == "__main__":
    registry = load()
    print(json.dumps(classify_diff(registry, sys.argv[1], sys.argv[2])))
