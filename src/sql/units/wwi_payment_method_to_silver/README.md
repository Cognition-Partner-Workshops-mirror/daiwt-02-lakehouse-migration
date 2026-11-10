# S04 Payment Method

The positional result is `wwi_payment_method_id`, `payment_method`, `valid_from`,
`valid_to`. Validity remains canonical seven-fractional-digit strings throughout.
Every change instant in base plus archive is reconstructed with start-inclusive,
end-exclusive AS OF semantics. Staged end times use the next strictly later
staged start for the same ID, otherwise the end-of-time sentinel.

## D0 baseline contract

Gold D0 remains a seeded baseline owned by G05. S04 independently extracts the
full temporal history over `(2012-12-31, 2016-06-06 12:31:17]` and strictly
compares its five versions with the D0 dimension excluding the unknown member.
D1 and D2 compare to new/changed golden dimension rows, and both are empty.
The job runs this multiset parity check after each successful pipeline update.

The bronze notebook is a unit-scoped replay adapter pending P4. It reconstructs
the requested snapshot from the immutable input chain, preserving archive
multiplicity and applying deletes before upserts. Raw audit rows are append-only
and retries do not duplicate already landed drops. The pipeline's history CTE
is the temporary P6 adapter. Replace both with merged Platform interfaces before
merge. No gold tables are owned or written by this unit.

Expectations warn without dropping rows; invalid staged rows are also copied to
`payment_method_changes_quarantine`. Golden verification requires it to be empty.
