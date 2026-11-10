#!/usr/bin/env bash
set -euo pipefail
suffix=${1:?usage: ci/dev_verify.sh pr_<number> [units]}
[[ $suffix =~ ^pr_[0-9]+$ ]] || { echo "invalid PR suffix" >&2; exit 2; }
[[ -z $(git status --porcelain) ]] || { echo "worktree must be clean" >&2; exit 1; }
git fetch -q origin
[[ -n $(git branch -r --contains HEAD) ]] || { echo "HEAD must be on a remote branch" >&2; exit 1; }
head_sha=$(git rev-parse HEAD)
mkdir -p out
rm -f out/parity-D{0,1,2}.json attestation.md
: > out/run-urls.txt
echo '[]' > out/selected-units.json
selection=changed
if [[ ${2:-} == '*' ]]; then selection='*'; elif [[ -n ${2:-} ]]; then selection=explicit; fi
status=PASS
if ! python - "${2:-}" > out/selected-units.json <<'PY'
import json, re, subprocess, sys
from pathlib import Path
sys.path.insert(0, "ci")
from classify_changes import classify, expand
registry = json.loads(Path("src/sql/units/registry.json").read_text())
if sys.argv[1]:
    units = expand(registry, re.split(r"[,\s]+", sys.argv[1].strip()))
else:
    base = subprocess.check_output(["git", "merge-base", "origin/main", "HEAD"], text=True).strip()
    paths = subprocess.check_output(["git", "diff", "--name-only", base, "HEAD"], text=True).splitlines()
    units = classify(registry, paths)
print(json.dumps(units))
PY
then
  echo '[]' > out/selected-units.json
  status=FAIL
fi
if [[ $status == PASS ]]; then
  if ! databricks bundle validate --strict -t dev --var "schema_suffix=$suffix" ||
     ! databricks bundle deploy -t dev --var "schema_suffix=$suffix"; then status=FAIL; fi
fi
if [[ $status == PASS ]]; then
  for D in D0 D1 D2; do
    if ! python ci/run_units.py "$suffix" out/selected-units.json --drop "$D" --run-urls out/run-urls.txt ||
       ! python src/parity/parity_check.py --suffix "$suffix" --drop "$D" --output "out/parity-$D.json"; then
      status=FAIL
      break
    fi
  done
fi
if ! python ci/attest.py out/parity-D0.json out/parity-D1.json out/parity-D2.json; then status=FAIL; fi
if ! python ci/record_attestation.py --head-sha "$head_sha" --suffix "$suffix" --status "$status" \
  --units out/selected-units.json --selection "$selection"; then status=FAIL; fi
[[ $status == PASS ]]
