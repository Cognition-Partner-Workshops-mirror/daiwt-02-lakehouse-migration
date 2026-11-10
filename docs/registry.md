# Unit registry

`src/sql/units/registry.json` maps unit names to job keys and table ownership.
For example (illustrative entries; register only implemented jobs):

```json
{
  "silver_city": {
    "job": "silver_city_job",
    "outputs": [],
    "owns": ["silver.city"],
    "depends_on": ["bronze"]
  },
  "gold_city": {
    "job": "gold_city_job",
    "outputs": ["gold.dim_city"],
    "owns": [],
    "depends_on": ["silver_city"]
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
determine DROP syntax; pipeline-managed objects are skipped for pipeline teardown.

Silver/Gold builders: add your entry; bronze tables already owned by the bronze unit must not be re-listed — depend on that unit instead.
