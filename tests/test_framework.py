import sqlite3

import pytest

from sql.framework import (END_OF_TIME, START_OF_TIME, csv_value, fmt_binary,
                           fmt_bit, fmt_date, fmt_datetime7, fmt_decimal,
                           identifier, lookup_dim_key, scd2)
from sql.framework.smoke import statements


def render_scd(**kwargs):
    args = dict(key="key", business_key="id", columns=["id", "valid_from", "valid_to"],
                staging_order=["stage_order"], key_high_water_mark=20)
    return scd2("dim", "stage", **{**args, **kwargs})


def test_scd2_preserves_every_staged_row_and_allocated_hwm():
    close, insert = render_scd()
    assert "MIN(`valid_from`)" in close
    assert f"t.`valid_to` = '{END_OF_TIME}'" in close
    assert "WHEN MATCHED THEN UPDATE SET `valid_to` = s.first_valid_from" in close
    db = sqlite3.connect(":memory:")
    db.executescript("CREATE TABLE dim (key INT, id INT, valid_from TEXT, valid_to TEXT);"
                     "CREATE TABLE stage (stage_order INT, id INT, valid_from TEXT, valid_to TEXT);")
    db.execute("INSERT INTO dim VALUES (7, 1, ?, ?)", (START_OF_TIME, END_OF_TIME))
    db.executemany("INSERT INTO stage VALUES (?, 1, ?, ?)",
                   [(2, START_OF_TIME, END_OF_TIME), (1, START_OF_TIME, END_OF_TIME)])
    db.execute(insert)
    assert db.execute("SELECT key FROM dim ORDER BY key").fetchall() == [(7,), (21,), (22,)]
    db.execute("DELETE FROM stage")
    db.execute(render_scd(key_high_water_mark=22)[1])
    assert db.execute("SELECT count(*) FROM dim").fetchone() == (3,)


@pytest.mark.parametrize("kwargs", [dict(key_high_water_mark=-1), dict(key_high_water_mark=True),
                                    dict(staging_order=[]), dict(columns=["id"]),
                                    dict(columns=["id", "valid_from", "valid_to", "id"])])
def test_scd2_invalid_parameters(kwargs):
    with pytest.raises(ValueError):
        render_scd(**kwargs)


@pytest.mark.parametrize("ts,expected", [(START_OF_TIME, 0), ("2016-01-01 00:00:00.0000000", 1),
                                       (END_OF_TIME, 2), (None, 0)])
def test_lookup_boundaries_and_first_match(ts, expected):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE dim (key INT, id INT, valid_from TEXT, valid_to TEXT)")
    db.executemany("INSERT INTO dim VALUES (?, 5, ?, ?)",
                   [(1, START_OF_TIME, "2016-01-01 00:00:00.0000000"),
                    (2, "2016-01-01 00:00:00.0000000", END_OF_TIME),
                    (3, "2016-01-02 00:00:00.0000000", END_OF_TIME)])
    db.execute("CREATE TABLE facts (fact_id INT, natural_id INT, modified TEXT)")
    db.executemany("INSERT INTO facts VALUES (?, ?, ?)", [(1, 5, ts), (2, None, ts), (3, 99, ts)])
    lookup = lookup_dim_key("dim", "facts", fact_row_key=["fact_id"], key="key", business_key="id",
                            natural_id="natural_id", timestamp="modified")
    assert "LEFT JOIN" in lookup and "LIMIT" not in lookup
    # SQLite has window functions but no QUALIFY; apply that filter in an outer query.
    select, qualify = lookup.split("\nQUALIFY ")
    select = select.replace("\nFROM", ", " + qualify.removesuffix(" = 1") + " AS rank\nFROM", 1)
    rows = db.execute(f"SELECT fact_id, dimension_key FROM ({select}) WHERE rank = 1 ORDER BY fact_id").fetchall()
    assert rows == [(1, expected), (2, 0), (3, 0)]
    db.execute("DELETE FROM facts")
    assert db.execute(f"SELECT * FROM ({select}) WHERE rank = 1").fetchall() == []


def test_formatters_and_smoke_contract():
    sql = "\n".join(statements("gold_pr_123"))
    assert END_OF_TIME in sql and "raise_error" in sql and "assert_true" in sql
    assert "20 + ROW_NUMBER()" in sql
    assert "9999999" in fmt_datetime7(f"'{END_OF_TIME}'")
    assert "TIMESTAMP_NTZ" in fmt_datetime7("value")
    assert "SSSSSS'), '0')" in fmt_datetime7("value", source="timestamp")
    assert fmt_date("x") == "date_format(x, 'yyyy-MM-dd')"
    assert fmt_decimal("x", scale=3) == "CAST(CAST((x) AS DECIMAL(18,3)) AS STRING)"
    assert fmt_binary("x") == "upper(hex(x))"
    assert fmt_bit("x") == "CAST(CAST((x) AS INT) AS STRING)"
    assert "chr(92)" in csv_value("NULL")


@pytest.mark.parametrize("call", [lambda: identifier("bad;drop"),
                                  lambda: fmt_datetime7("x", source="bad"),
                                  lambda: fmt_decimal("x", scale=39),
                                  lambda: lookup_dim_key("dim", "facts", fact_row_key=[], key="key", business_key="id", natural_id="id", timestamp="ts"),
                                  lambda: list(statements("gold"))])
def test_invalid_sql_parameters(call):
    with pytest.raises(ValueError):
        call()
