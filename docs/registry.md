# Unit registry

PASS attestations may carry over across merges or rebases only when PR-owned paths, blobs (except removal of `users` permission grants from resource YAML), and registry-entry changes are identical and recorded units still cover the current selection.

`src/sql/units/registry.json` maps unit names to job keys and table ownership.
For example (illustrative entries; register only implemented jobs):

```json
{
  "wwi_city_to_silver": {
    "job": "wwi_city_to_silver",
    "outputs": [],
    "owns": ["silver.city_changes", "silver.city_changes_quarantine"],
    "depends_on": ["bronze_landing_replay"]
  },
  "wwi_dim_city": {
    "job": "wwi_dim_city",
    "outputs": ["gold.dim_city"],
    "owns": [],
    "depends_on": ["wwi_city_to_silver"]
  }
}
```

All four fields are required; extra fields are rejected. Unit names use
`[a-z][a-z0-9_]*`; jobs are non-empty and unique. Tables use an unsuffixed
`bronze`, `silver`, or `gold` layer and a lowercase identifier. Each table has
one owner, with no duplicates. Contract tables belong in `outputs`; all other
tables belong in `owns`. Dependencies must exist and form an acyclic graph.

Classification expands to dependants; classification and job execution order
selected units dependencies-first, breaking ties by registry order. Cleanup
uses both lists: `--suffix pr_123 --units a,b` (or `*`) drops dev objects only;
`--prod` drops all registered objects without destroying schemas. Object types
determine DROP syntax; pipeline-managed objects are skipped, so units backed by a
Lakeflow pipeline must full-refresh it on the D0 run (reruns then start clean).

Silver/Gold builders: add your entry; bronze tables already owned by the bronze unit must not be re-listed — depend on that unit instead.
