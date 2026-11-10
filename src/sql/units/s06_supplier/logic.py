"""S06 stand-ins for P4 replay and P6 temporal extraction; CSV values stay exact."""
import csv
from collections import Counter
from pathlib import Path

END = '9999-12-31 23:59:59.9999999'
COLUMNS = ('wwi_supplier_id', 'supplier', 'category', 'primary_contact',
           'supplier_reference', 'payment_days', 'postal_code', 'valid_from', 'valid_to')
SOURCES = {
    'Purchasing.Suppliers': ('purchasing_suppliers', 'SupplierID'),
    'Purchasing.SupplierCategories': ('purchasing_supplier_categories', 'SupplierCategoryID'),
    'Application.People': ('application_people', 'PersonID'),
}
SOURCES = {name + archive: (table + suffix, None if archive else key)
           for name, (table, key) in SOURCES.items()
           for archive, suffix in [('', ''), ('_Archive', '_archive')]}


def read_csv(path):
    with Path(path).open(newline='', encoding='utf-8-sig') as stream:
        return list(csv.DictReader(stream))


def replay(current, additions, deleted, key):
    if key:
        state = {r[key]: r for r in current}
        for row in deleted:
            state.pop(row[key], None)
        state.update((r[key], r) for r in additions)
        return list(state.values())
    columns = tuple((current or additions or deleted or [{}])[0])
    counts = Counter(tuple(r[c] for c in columns) for r in current)
    removals = Counter(tuple(r[c] for c in columns) for r in deleted)
    if removals - counts:
        raise ValueError('archive delete not present in current multiset')
    counts.subtract(removals)
    counts.update(tuple(r[c] for c in columns) for r in additions)
    return [dict(zip(columns, values)) for values, n in counts.items() for _ in range(n)]


def cutoffs(manifest, drop):
    entry = next(d for d in manifest['drops'] if d['name'] == drop)
    def value(item):
        if isinstance(item, list):
            item = next(r['Cutoff Time'] for r in item if r['Table Name'] == 'Supplier')
        return item.split('.')[0] + '.' + (item.split('.')[1] if '.' in item else '').ljust(7, '0')
    return value(entry['last_cutoff']), value(entry['new_cutoff'])


def walk(suppliers, categories, people, last, new):
    staged = []
    for rows, key, exclude_initial in [(categories, 'SupplierCategoryID', True),
                                       (suppliers, 'SupplierID', False)]:
        for event in rows:
            t = event['ValidFrom']
            if not last < t <= new or (exclude_initial and t == '2020-01-01 00:00:00.0000000'):
                continue
            for s in suppliers:
                if s[key] != event[key] or not s['ValidFrom'] <= t < s['ValidTo']:
                    continue
                for c in categories:
                    if c['SupplierCategoryID'] != s['SupplierCategoryID'] or not c['ValidFrom'] <= t < c['ValidTo']:
                        continue
                    for p in people:
                        if p['PersonID'] == s['PrimaryContactPersonID'] and p['ValidFrom'] <= t < p['ValidTo']:
                            staged.append((s['SupplierID'], s['SupplierName'], c['SupplierCategoryName'],
                                           p['FullName'], s['SupplierReference'], s['PaymentDays'],
                                           s['DeliveryPostalCode'], s['ValidFrom'], END))
    return [r[:-1] + (min((s[7] for s in staged if s[0] == r[0] and s[7] > r[7]), default=END),)
            for r in staged]


def expected(root, drop):
    if drop == 'D0':
        return Counter()
    previous = f'D{int(drop[1:]) - 1}'
    def rows(d):
        return Counter(tuple(r[c] for c in COLUMNS) for r in
                       read_csv(Path(root) / 'outputs' / d / 'tables/Dimension.Supplier.csv'))
    return rows(drop) - rows(previous)


def change_sql(bronze, last, new):
    def history(table):
        return f'SELECT * FROM {bronze}.{table} UNION ALL SELECT * FROM {bronze}.{table}_archive'
    return f"""
WITH s AS ({history('purchasing_suppliers')}),
c AS ({history('purchasing_supplier_categories')}),
p AS ({history('application_people')}),
events AS (
 SELECT 's' kind, SupplierID id, ValidFrom t FROM s
 WHERE ValidFrom > '{last}' AND ValidFrom <= '{new}'
 UNION ALL
 SELECT 'c', SupplierCategoryID, ValidFrom FROM c
 WHERE ValidFrom > '{last}' AND ValidFrom <= '{new}'
 AND ValidFrom <> '2020-01-01 00:00:00.0000000'
), staged AS (
 SELECT s.SupplierID wwi_supplier_id, s.SupplierName supplier,
 c.SupplierCategoryName category, p.FullName primary_contact,
 s.SupplierReference supplier_reference, s.PaymentDays payment_days,
 s.DeliveryPostalCode postal_code, s.ValidFrom valid_from
 FROM events e JOIN s ON s.ValidFrom <= e.t AND e.t < s.ValidTo
 AND ((e.kind = 's' AND s.SupplierID = e.id) OR
      (e.kind = 'c' AND s.SupplierCategoryID = e.id))
 JOIN c ON c.SupplierCategoryID = s.SupplierCategoryID AND c.ValidFrom <= e.t AND e.t < c.ValidTo
 JOIN p ON p.PersonID = s.PrimaryContactPersonID AND p.ValidFrom <= e.t AND e.t < p.ValidTo
), ends AS (
 SELECT a.wwi_supplier_id, a.valid_from, min(b.valid_from) valid_to
 FROM staged a LEFT JOIN staged b ON a.wwi_supplier_id = b.wwi_supplier_id
 AND b.valid_from > a.valid_from GROUP BY a.wwi_supplier_id, a.valid_from
)
SELECT staged.*, coalesce(ends.valid_to, '{END}') valid_to FROM staged
JOIN ends USING (wwi_supplier_id, valid_from)
"""
