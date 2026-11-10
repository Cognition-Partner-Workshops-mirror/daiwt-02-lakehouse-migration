"""Reusable Databricks SQL renderers for gold jobs.

Store datetime2(7) as fixed-width STRING yyyy-MM-dd HH:mm:ss.fffffff,
including validity bounds and fact Last Modified When. Never cast source strings
through TIMESTAMP: Spark loses the seventh digit. Lexical range comparisons then
preserve SQL Server precision. Native timestamps can only supply microseconds;
formatting appends zero, not a reconstructed seventh digit. Use TIMESTAMP_NTZ for
naive source times (or set the session timezone explicitly for TIMESTAMP).

Execute scd2 statements in order, with one writer per dimension and an immutable
staging snapshot. staging_order must uniquely order ALL staged rows. The caller
supplies the allocated key high-water mark, not a recomputed MAX after deletions;
persist the returned allocation boundary (old HWM + staged count) in job state.
For a freshly seeded D0 table its MAX key is the initial HWM. Golden's sole new
dimension allocation is Employee D2: 212 -> 213; other dims allocate no new keys.
No transaction/retry guarantees are implied: replay must restore pre-drop state.

SQL expressions are trusted job code; identifiers are validated and quoted.
The renderers return SQL for spark.sql or the SQL Statements API, not Python ETL.
"""

import re

END_OF_TIME = "9999-12-31 23:59:59.9999999"
START_OF_TIME = "2013-01-01 00:00:00.0000000"


def identifier(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name}")
    return f"`{name}`"


def relation(name):
    return ".".join(identifier(part) for part in name.split("."))


def scd2(target, staging, *, key, business_key, columns, staging_order,
         key_high_water_mark, valid_from="valid_from", valid_to="valid_to"):
    """Close current versions, then append every stage row with sequential keys.

    columns are staging/target columns excluding the surrogate key (including
    lineage if required). Stage validity ends are copied unchanged. HWM is the
    last allocated key, so the first new key is HWM + 1, even if rows were deleted.
    """
    if type(key_high_water_mark) is not int or key_high_water_mark < 0:
        raise ValueError("key_high_water_mark must be a nonnegative integer")
    if not staging_order or not columns or len(set(columns)) != len(columns):
        raise ValueError("columns must be unique and staging_order nonempty")
    if key in columns or not {business_key, valid_from, valid_to} <= set(columns):
        raise ValueError("columns must include business key and validity, excluding surrogate key")
    t, s = relation(target), relation(staging)
    k, bk, vf, vt = map(identifier, (key, business_key, valid_from, valid_to))
    names = ", ".join(identifier(c) for c in columns)
    order = ", ".join(identifier(c) for c in staging_order)
    close = f"""MERGE INTO {t} AS t
USING (SELECT {bk}, MIN({vf}) AS first_valid_from FROM {s} GROUP BY {bk}) AS s
ON t.{bk} = s.{bk} AND t.{vt} = '{END_OF_TIME}'
WHEN MATCHED THEN UPDATE SET {vt} = s.first_valid_from"""
    insert = f"""INSERT INTO {t} ({k}, {names})
SELECT {key_high_water_mark} + ROW_NUMBER() OVER (ORDER BY {order}), {names}
FROM {s}"""
    return close, insert


def lookup_dim_key(dimension, *, key, business_key, natural_id, timestamp,
                   valid_from="valid_from", valid_to="valid_to"):
    """Scalar expression; natural_id/timestamp are trusted outer SQL expressions.

    Use fact Last Modified When for timestamp. Equal valid_from ties are resolved
    by surrogate key, after the required earliest-valid_from ordering.
    """
    dim = relation(dimension)
    k, bk, vf, vt = map(identifier, (key, business_key, valid_from, valid_to))
    return f"""COALESCE((SELECT d.{k} FROM {dim} AS d
WHERE d.{bk} = ({natural_id}) AND ({timestamp}) > d.{vf}
AND ({timestamp}) <= d.{vt}
ORDER BY d.{vf}, d.{k} LIMIT 1), 0)"""


def fmt_datetime7(expression, *, source="string"):
    """Canonical text, preserving the seventh digit; NULL remains SQL NULL.

    Strings accept a space or T separator and zero to seven fraction digits.
    Malformed strings fail instead of silently losing precision.
    """
    if source == "timestamp":
        return f"concat(date_format({expression}, 'yyyy-MM-dd HH:mm:ss.SSSSSS'), '0')"
    if source != "string":
        raise ValueError("source must be string or timestamp")
    value = f"replace(({expression}), 'T', ' ')"
    pattern = "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{1,7})?$"
    return f"""CASE WHEN ({expression}) IS NULL THEN NULL
WHEN {value} RLIKE '{pattern}' AND try_cast(substr({value}, 1, 19) AS TIMESTAMP_NTZ) IS NOT NULL
THEN concat(substr({value}, 1, 19), '.', rpad(substr({value}, 21, 7), 7, '0'))
ELSE raise_error('Invalid datetime2(7) string') END"""


def fmt_date(expression):
    return f"date_format({expression}, 'yyyy-MM-dd')"


def fmt_decimal(expression, *, precision=18, scale=2):
    if type(precision) is not int or type(scale) is not int or not 0 <= scale <= precision <= 38 or precision < 1:
        raise ValueError("invalid DECIMAL precision/scale")
    return f"CAST(CAST(({expression}) AS DECIMAL({precision},{scale})) AS STRING)"


def fmt_binary(expression):
    return f"upper(hex({expression}))"


def fmt_bit(expression):
    return f"CAST(CAST(({expression}) AS INT) AS STRING)"


def csv_value(expression):
    """Wrap a formatted expression for comparison/export; empty string != NULL.

    chr(92) avoids dependence on SQL string-literal backslash escaping. For actual
    CSV output use the writer's nullValue='\\N' instead of pre-encoding NULLs.
    """
    return f"coalesce(CAST(({expression}) AS STRING), concat(chr(92), 'N'))"
