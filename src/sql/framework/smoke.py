"""Render smoke SQL: python -m sql.framework.smoke gold_pr_123.

Execute returned statements sequentially on warehouse 6dc4b80538621b7b. Objects
are restricted to daiwt_02_mig_dev.gold_pr_<n>; no bundle resource is required.
The final statements drop only this fixture's tables. Assertion errors deliberately
stop execution, leaving evidence in the caller's scratch schema for inspection.
"""

import argparse
import re

from . import (END_OF_TIME, START_OF_TIME, csv_value, fmt_binary, fmt_bit,
               fmt_date, fmt_datetime7, fmt_decimal, lookup_dim_key, scd2)


def statements(gold_schema):
    if not re.fullmatch(r"gold_pr_[0-9]+", gold_schema):
        raise ValueError("smoke requires a dev gold_pr_<n> schema")
    target = f"daiwt_02_mig_dev.{gold_schema}.p3_smoke_dim"
    stage = f"daiwt_02_mig_dev.{gold_schema}.p3_smoke_stage"
    facts = f"daiwt_02_mig_dev.{gold_schema}.p3_smoke_facts"
    yield f"CREATE OR REPLACE TABLE {target} (dim_key BIGINT, natural_id INT, label STRING, valid_from STRING, valid_to STRING) USING DELTA"
    yield f"INSERT INTO {target} VALUES (7, 1, 'same', '{START_OF_TIME}', '{END_OF_TIME}')"
    yield f"CREATE OR REPLACE TABLE {stage} (stage_order INT, natural_id INT, label STRING, valid_from STRING, valid_to STRING) USING DELTA"
    yield f"""INSERT INTO {stage} VALUES
(2, 1, 'same', '2016-01-02 00:00:00.0000000', '{END_OF_TIME}'),
(1, 1, 'same', '2016-01-01 00:00:00.0000000', '2016-01-02 00:00:00.0000000')"""
    args = dict(key="dim_key", business_key="natural_id",
                columns=["natural_id", "label", "valid_from", "valid_to"],
                staging_order=["stage_order"], key_high_water_mark=20)
    yield from scd2(target, stage, **args)
    yield f"SELECT assert_true(count(*) = 3 AND max(dim_key) = 22) FROM {target}"
    yield f"SELECT assert_true(valid_to = '2016-01-01 00:00:00.0000000') FROM {target} WHERE dim_key = 7"
    yield f"CREATE OR REPLACE TABLE {facts} (fact_id INT, natural_id INT, last_modified_when STRING, expected_key BIGINT) USING DELTA"
    yield f"""INSERT INTO {facts} VALUES
(1, 1, '{START_OF_TIME}', 0),
(2, 1, '2016-01-01 00:00:00.0000000', 7),
(3, 1, '2016-01-02 00:00:00.0000000', 21),
(4, 1, '{END_OF_TIME}', 22),
(5, NULL, '{END_OF_TIME}', 0),
(6, 99, '{END_OF_TIME}', 0),
(7, 1, NULL, 0)"""
    lookup = lookup_dim_key(target, facts, fact_row_key=["fact_id"], key="dim_key",
                            business_key="natural_id", natural_id="natural_id",
                            timestamp="last_modified_when")
    yield f"SELECT CASE WHEN count(*) = 7 AND count_if(dimension_key != expected_key) = 0 THEN 'PASS' ELSE raise_error('lookup boundary failure') END FROM ({lookup})"
    for expression, expected in [
        (fmt_datetime7(f"'{END_OF_TIME}'"), END_OF_TIME),
        (fmt_datetime7("'2016-01-01T01:02:03.1234567'"), "2016-01-01 01:02:03.1234567"),
        (fmt_datetime7("TIMESTAMP_NTZ'2016-01-01 01:02:03.123456'", source="timestamp"), "2016-01-01 01:02:03.1234560"),
        (fmt_date("DATE'2016-01-01'"), "2016-01-01"),
        (fmt_decimal("15", scale=3), "15.000"),
        (fmt_binary("unhex('0aff')"), "0AFF"), (fmt_bit("true"), "1"),
        (csv_value("NULL"), None), (csv_value("''"), ""),
    ]:
        rhs = "concat(chr(92), 'N')" if expected is None else f"'{expected}'"
        yield f"SELECT assert_true(({expression}) = {rhs})"
    yield f"TRUNCATE TABLE {stage}"
    yield from scd2(target, stage, **{**args, "key_high_water_mark": 22})
    yield f"SELECT assert_true(count(*) = 3 AND max(dim_key) = 22) FROM {target}"
    yield f"DROP TABLE {stage}"
    yield f"DROP TABLE {facts}"
    yield f"DROP TABLE {target}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gold_schema")
    args = parser.parse_args()
    print(";\n\n".join(statements(args.gold_schema)) + ";")
