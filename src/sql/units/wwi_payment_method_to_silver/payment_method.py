import csv
from collections import Counter
from pathlib import Path

SOURCE_COLUMNS = ('PaymentMethodID', 'PaymentMethodName', 'LastEditedBy', 'ValidFrom', 'ValidTo')
COLUMNS = ('wwi_payment_method_id', 'payment_method', 'valid_from', 'valid_to')
END = '9999-12-31 23:59:59.9999999'
DROPS = ('D0', 'D1', 'D2')


def window(drop, last_cutoff, new_cutoff):
    if drop == 'D0':
        last_cutoff = '2012-12-31 00:00:00.0000000'
    if drop not in DROPS or not last_cutoff or not new_cutoff or last_cutoff >= new_cutoff:
        raise ValueError('Invalid drop or cutoff window')
    return drop, last_cutoff, new_cutoff


def read_csv(path):
    with Path(path).open(newline='', encoding='utf-8-sig') as stream:
        return [{key: None if value == r'\N' else value for key, value in row.items()}
                for row in csv.DictReader(stream)]


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
