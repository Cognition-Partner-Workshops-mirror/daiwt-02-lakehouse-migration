# G09 — Movement

D0 restores the P5 baseline for gold.fact_movement. D1 and D2 read the
S08 movement window and merge on WWI Stock Item Transaction ID. Matched
rows retain their movement key; new keys follow the current high-water
mark in ascending staged transaction ID order.

P3 lookups use transaction occurrence time, with an exclusive valid_from
and inclusive valid_to, earliest valid_from first, and fallback key 0.
Quantity is the integer projection supplied by S08; this fact has no
decimal measures. P5 supplies the Movement lineage key, completed only
after a successful merge. The parity task compares the full table with
the shared golden contract for each drop.

Local checks: `pytest -q`.
