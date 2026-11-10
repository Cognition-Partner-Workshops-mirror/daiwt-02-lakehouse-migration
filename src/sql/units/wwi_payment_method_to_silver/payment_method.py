import csv
from collections import Counter
from pathlib import Path

SOURCE_COLUMNS = ('PaymentMethodID', 'PaymentMethodName', 'LastEditedBy', 'ValidFrom', 'ValidTo')
COLUMNS = ('wwi_payment_method_id', 'payment_method', 'valid_from', 'valid_to')
END = '9999-12-31 23:59:59.9999999'
DROPS = ('D0', 'D1', 'D2')


def read_csv(path):
    with Path(path).open(newline='', encoding='utf-8-sig') as stream:
        return [{key: None if value == r'\N' else value for key, value in row.items()}
                for row in csv.DictReader(stream)]


def replay(root, drop, archive=False):
    name = 'PaymentMethods_Archive' if archive else 'PaymentMethods'
    snapshot = []
    audit = []
    for current in DROPS[:DROPS.index(drop) + 1]:
        path = Path(root) / 'inputs' / current / f'WideWorldImporters.Application.{name}.csv'
        deleted_path = path.with_suffix('.deleted.csv')
        deleted = read_csv(deleted_path) if deleted_path.exists() else []
        incoming = read_csv(path)
        for row in deleted:
            matches = [i for i, old in enumerate(snapshot)
                       if old == row or (not archive and old['PaymentMethodID'] == row['PaymentMethodID'])]
            if not matches:
                raise ValueError('Delete does not match a source row')
            snapshot.pop(matches[0])
        if not archive:
            keys = {row['PaymentMethodID'] for row in incoming}
            snapshot = [row for row in snapshot if row['PaymentMethodID'] not in keys]
        snapshot.extend(incoming)
        for op, rows, source in [('delete', deleted, deleted_path),
                                 ('base' if current == 'D0' else 'upsert', incoming, path)]:
            audit.extend({**row, '_drop': current, '_op': op, '_source_file': str(source),
                          '_row_ordinal': str(i)} for i, row in enumerate(rows, 1))
    return snapshot, audit


def changes(history, last_cutoff, new_cutoff):
    if last_cutoff >= new_cutoff:
        raise ValueError('Cutoff window must increase')
    events = [(r['PaymentMethodID'], r['ValidFrom']) for r in history
              if last_cutoff < r['ValidFrom'] <= new_cutoff]
    staged = [dict(wwi_payment_method_id=int(r['PaymentMethodID']), payment_method=r['PaymentMethodName'],
                   valid_from=r['ValidFrom'])
              for key, instant in events for r in history
              if r['PaymentMethodID'] == key and r['ValidFrom'] <= instant < r['ValidTo']]
    return [{**row, 'valid_to': min((other['valid_from'] for other in staged
                                   if other['wwi_payment_method_id'] == row['wwi_payment_method_id']
                                   and other['valid_from'] > row['valid_from']), default=END)}
            for row in sorted(staged, key=lambda row: row['valid_from'])]


def expected(root, drop):
    def load(current):
        return read_csv(Path(root) / 'outputs' / current / 'tables' / 'Dimension.Payment_Method.csv')
    previous = {r['payment_method_key']: r for r in load(DROPS[DROPS.index(drop) - 1])} if drop != 'D0' else {}
    return [{column: row[column] for column in COLUMNS} for row in load(drop)
            if row['payment_method_key'] != '0' and previous.get(row['payment_method_key']) != row]


def assert_parity(actual, golden):
    def bag(rows):
        return Counter(tuple(None if row[c] is None else str(row[c]) for c in COLUMNS) for row in rows)
    if bag(actual) != bag(golden):
        raise AssertionError(f'Payment method multiset mismatch: actual={bag(actual)}, expected={bag(golden)}')
