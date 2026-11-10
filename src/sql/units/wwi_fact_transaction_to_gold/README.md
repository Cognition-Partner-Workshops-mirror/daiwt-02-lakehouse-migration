# G14 — Transaction

D0 restores the P5 baseline. D1/D2 append every requested extraction occurrence
from S13 transaction_changes, without deleting, merging or deduplicating prior
transactions. Replaying a drop appends again; restore D0 before a full replay.

Surrogates continue after the current fact high-water mark. `staged_order()`
isolates the deterministic ordering: customer before supplier, Last Modified
When, source transaction ID. This does not reproduce SQL Server physical row
order; exact key-inclusive D1/D2 parity is pending an ordering decision. The
parity task deliberately retains strict comparison, including transaction_key.

P3 dimension lookups use Last Modified When, strict lower/inclusive upper
validity bounds and unknown key 0, choosing the earliest matching valid_from.
S13 retains both customer roles and the transaction date. Monetary values pass
through STRING before decimal(18,2). P5 Integration supplies Transaction's
per-cutoff lineage and records completion only after a successful append.

The job accepts the runner's D0/D1/D2 parameters and exposes load and parity as
separate tasks. Silver and dimension dependencies are declared in the registry.
