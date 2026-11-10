"""S10 source replay and local reference; datetime2(7) values stay strings."""
import csv
import json
from collections import Counter
from pathlib import Path

SOURCES = {
    'Purchasing.PurchaseOrders': 'purchasing_purchase_orders',
    'Purchasing.PurchaseOrderLines': 'purchasing_purchase_order_lines',
    'Warehouse.StockItems': 'warehouse_stock_items',
    'Warehouse.StockItems_Archive': 'warehouse_stock_items_archive',
    'Warehouse.PackageTypes': 'warehouse_package_types',
    'Warehouse.PackageTypes_Archive': 'warehouse_package_types_archive',
}
COLUMNS = ('date_key', 'wwi_purchase_order_id', 'ordered_outers',
           'ordered_quantity', 'received_outers', 'package', 'is_order_finalized',
           'wwi_supplier_id', 'wwi_stock_item_id', 'last_modified_when')
PROJECT = COLUMNS[:7]


def read_csv(root, path):
    with (Path(root) / path).open(newline='', encoding='utf-8-sig') as handle:
        return [{k: None if v == '\\N' else v for k, v in row.items()}
                for row in csv.DictReader(handle)]


def manifest(root):
    return json.loads((Path(root) / 'manifest.json').read_text())


def input_path(drop, source):
    return f'inputs/{drop}/WideWorldImporters.{source}.csv'


def apply_batch(state, upserts, deletes, columns, pk):
    if pk:
        key = lambda row: tuple(row[c] for c in pk)
        current = {key(row): row for row in state}
        for row in deletes:
            current.pop(key(row), None)
        current.update((key(row), row) for row in upserts)
        return list(current.values())
    key = lambda row: tuple(row[c] for c in columns)
    current = Counter(key(row) for row in state)
    removed = Counter(key(row) for row in deletes)
    if removed - current:
        raise ValueError('archive deletion is not present in current state')
    current.subtract(removed)
    current.update(key(row) for row in upserts)
    return [dict(zip(columns, row)) for row, count in current.items() for _ in range(count)]


def replay(root, spec):
    states = {}
    for source in SOURCES:
        state = []
        for drop in ('D0', 'D1', 'D2'):
            path = input_path(drop, source)
            info = spec['files'][path]
            deleted = path[:-4] + '.deleted.csv'
            state = apply_batch(state, read_csv(root, path),
                                read_csv(root, deleted) if deleted in spec['files'] else [],
                                info['columns'], info['primary_key'])
            if len(state) != info['full_row_count']:
                raise ValueError(f'{path}: expected {info["full_row_count"]}, actual {len(state)}')
            states[drop, source] = state
    return states


def cutoff(drop, field):
    value = drop[field]
    if isinstance(value, list):
        value = next(r['Cutoff Time'] for r in value if r['Table Name'] == 'Purchase')
    return value if '.' in value else value + '.0000000'


def extract(states, spec, drop):
    orders = {r['PurchaseOrderID']: r for r in states[drop, 'Purchasing.PurchaseOrders']}
    items = {r['StockItemID']: r for r in states[drop, 'Warehouse.StockItems']}
    packages = {r['PackageTypeID']: r for r in states[drop, 'Warehouse.PackageTypes']}
    window = next(d for d in spec['drops'] if d['name'] == drop)
    result, orphans = [], []
    for line in states[drop, 'Purchasing.PurchaseOrderLines']:
        po = orders.get(line['PurchaseOrderID'])
        modified = (line['LastEditedWhen'] if po and line['LastEditedWhen'] > po['LastEditedWhen']
                    else po['LastEditedWhen'] if po else line['LastEditedWhen'])
        if not cutoff(window, 'last_cutoff') < modified <= cutoff(window, 'new_cutoff'):
            continue
        si, pt = items.get(line['StockItemID']), packages.get(line['PackageTypeID'])
        if not po or not si or not pt:
            orphans.append(line)
            continue
        values = (po['OrderDate'], po['PurchaseOrderID'], line['OrderedOuters'],
                  str(int(line['OrderedOuters']) * int(si['QuantityPerOuter'])),
                  line['ReceivedOuters'], pt['PackageTypeName'], line['IsOrderLineFinalized'],
                  po['SupplierID'], line['StockItemID'], modified)
        result.append(dict(zip(COLUMNS, values), _drop=drop,
                           purchase_order_line_id=line['PurchaseOrderLineID']))
    result.sort(key=lambda r: (int(r['wwi_purchase_order_id']), int(r['purchase_order_line_id'])))
    return [dict(row, _staged_order=i) for i, row in enumerate(result, 1)], orphans


def column_type(name):
    if name.endswith('ID') or name in ('OrderedOuters', 'ReceivedOuters', 'QuantityPerOuter',
                                      'LeadTimeDays', 'LastEditedBy') or name.startswith('Is'):
        return 'INT'
    if name in ('OrderDate', 'ExpectedDeliveryDate', 'LastReceiptDate'):
        return 'DATE'
    if name in ('ExpectedUnitPricePerOuter', 'UnitPrice', 'RecommendedRetailPrice'):
        return 'DECIMAL(18,2)'
    if name in ('TaxRate', 'TypicalWeightPerUnit'):
        return 'DECIMAL(18,3)'
    return 'STRING'
