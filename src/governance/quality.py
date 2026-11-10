"""Warn-only expectations and non-destructive, per-drop quarantine copies."""

from . import identifier, literal, relation


def failed_rules_expr(rules):
    if not rules:
        return "CAST(array() AS ARRAY<STRING>)"
    failures = ", ".join(f"CASE WHEN coalesce(({expr}), false) THEN NULL ELSE {literal(name)} END"
                         for name, expr in rules.items())
    return f"filter(array({failures}), rule -> rule IS NOT NULL)"


def quarantine_sql(source, quarantine, rules, drop=None):
    """Copy failures from a silver source with its standard STRING _drop column.

    An explicit drop scopes replacement to that drop. None replaces all drops
    currently present in the source, leaving other historical drops untouched.
    """
    if source.lower() == quarantine.lower():
        raise ValueError("quarantine must differ from source")
    s, q = relation(source), relation(quarantine)
    failures = failed_rules_expr(rules)
    selection = f"s.* EXCEPT (_drop), {failures} AS _failed_rules, s._drop, current_timestamp() AS _quarantined_at"
    predicate = "true" if drop is None else f"s.{identifier('_drop')} <=> {literal(drop)}"
    delete = (f"EXISTS (SELECT 1 FROM {s} s WHERE s._drop <=> q._drop)"
              if drop is None else f"q._drop <=> {literal(drop)}")
    return [
        f"CREATE TABLE IF NOT EXISTS {q} USING DELTA AS SELECT {selection} FROM {s} s WHERE false",
        f"DELETE FROM {q} q WHERE {delete}",
        f"INSERT INTO {q} SELECT {selection} FROM {s} s WHERE ({predicate}) AND size({failures}) > 0",
    ]


def warn_expectations(rules):
    return {name: f"coalesce(({expression}), false)" for name, expression in rules.items()}


def quarantine_frame(df, rules):
    from pyspark.sql import functions as F

    return df.withColumn("_failed_rules", F.expr(failed_rules_expr(rules))).where(F.size("_failed_rules") > 0)
