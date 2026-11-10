#!/usr/bin/env bash
set -euo pipefail
suffix=${1:?usage: ci/dev_verify.sh pr_<number>}
[[ $suffix =~ ^pr_[0-9]+$ ]] || { echo "invalid PR suffix" >&2; exit 2; }
rm -f out/parity.json
mkdir -p out
databricks bundle validate --strict -t dev --var "schema_suffix=$suffix"
databricks bundle deploy -t dev --var "schema_suffix=$suffix"
bash ci/classify_changes.sh > out/changed-units.json
python ci/run_units.py "$suffix" out/changed-units.json
python src/parity/parity_check.py --suffix "$suffix"
