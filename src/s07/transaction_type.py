"""Transaction Type temporal extraction and golden contract (datetime2 text is exact)."""
from collections import Counter

SOURCE_COLUMNS = ("TransactionTypeID", "TransactionTypeName", "LastEditedBy", "ValidFrom", "ValidTo")
COLUMNS = ("wwi_transaction_type_id", "transaction_type", "valid_from", "valid_to")
END_OF_TIME = "9999-12-31 23:59:59.9999999"
INITIAL_CUTOFF = "2012-12-31 00:00:00.0000000"


def window(manifest, drop):
    entry = next(d for d in manifest["drops"] if d["name"] == drop)
    def cutoff(name):
        value = entry[name]
        if isinstance(value, list):
            value = next(r["Cutoff Time"] for r in value if r["Table Name"] == "Transaction Type")
        return value if "." in value else value + ".0000000"
    return INITIAL_CUTOFF if drop == "D0" else cutoff("last_cutoff"), cutoff("new_cutoff")


def extract(versions, last_cutoff, new_cutoff):
    staged = []
    for event in versions:
        t = event["ValidFrom"]
        if last_cutoff < t <= new_cutoff:
            for row in versions:
                if (row["TransactionTypeID"] == event["TransactionTypeID"]
                        and row["ValidFrom"] <= t < row["ValidTo"]):
                    staged.append((str(row["TransactionTypeID"]), row["TransactionTypeName"], row["ValidFrom"]))
    return [(*row, min((other[2] for other in staged if other[0] == row[0] and other[2] > row[2]), default=END_OF_TIME))
            for row in sorted(staged, key=lambda r: (r[2], int(r[0])))]


def expected_rows(current, previous=None):
    def project(rows):
        return Counter(tuple(row[c] for c in COLUMNS) for row in rows if row["transaction_type_key"] != "0")
    return project(current) if previous is None else project(current) - project(previous)


def assert_parity(actual, expected, quarantine_count=0):
    if Counter(actual) != expected or quarantine_count:
        raise AssertionError(f"S07 parity mismatch: actual={Counter(actual)}, expected={expected}, quarantine={quarantine_count}")
