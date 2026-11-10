"""Drop snapshots derived from P4 raw history, without owning bronze objects."""
from purchase_core import SOURCES, column_type, input_path


def snapshot_sql(catalog, bronze, source, drop, spec):
    if drop not in ('D0', 'D1', 'D2') or source not in SOURCES:
        raise ValueError('Unsupported purchase source/drop')
    info = spec['files'][input_path(drop, source)]
    columns = info['columns']
    names = ', '.join(f'`{c}`' for c in columns)
    typed = ', '.join(f'CAST(`{c}` AS {column_type(c)}) AS `{c}`' for c in columns)
    raw = f'`{catalog}`.`{bronze}`.`{SOURCES[source]}_raw`'
    history = f"SELECT * FROM {raw} WHERE _drop <= '{drop}'"
    if info['primary_key']:
        keys = ', '.join(f'`{c}`' for c in info['primary_key'])
        return f"""SELECT {typed}, '{drop}' AS _drop FROM (
            SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY _drop DESC,
                CASE WHEN _op = 'delete' THEN 0 ELSE 1 END DESC) AS _version
            FROM ({history})
        ) WHERE _version = 1 AND _op <> 'delete'"""
    return f"""SELECT {typed}, '{drop}' AS _drop FROM (
        SELECT {names}, sum(CASE WHEN _op = 'delete' THEN -1 ELSE 1 END) AS _copies
        FROM ({history}) GROUP BY {names}
    ) LATERAL VIEW explode(CASE WHEN _copies >= 0 THEN array_repeat(1, CAST(_copies AS INT))
        ELSE raise_error('Negative purchase archive multiplicity') END) copies AS _copy"""


def completed_drop_sql(catalog, bronze, drop):
    return f"""SELECT count(DISTINCT bronze_table) = 38
        FROM `{catalog}`.`{bronze}`.bronze_replay_log
        WHERE drop = '{drop}' AND status = 'PASS'"""
