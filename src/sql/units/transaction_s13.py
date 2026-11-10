"""Transaction extraction contract shared by replay, Lakeflow and offline tests."""
import csv
from collections import Counter

SOURCES = {
    'Sales.CustomerTransactions': 'sales_customer_transactions',
    'Purchasing.SupplierTransactions': 'purchasing_supplier_transactions',
    'Sales.Invoices': 'sales_invoices',
}
NAMES = ['Date Key', 'WWI Customer Transaction ID', 'WWI Supplier Transaction ID',
         'WWI Invoice ID', 'WWI Purchase Order ID', 'Supplier Invoice Number',
         'Total Excluding Tax', 'Tax Amount', 'Total Including Tax', 'Outstanding Balance',
         'Is Finalized', 'WWI Customer ID', 'WWI Bill To Customer ID', 'WWI Supplier ID',
         'WWI Transaction Type ID', 'WWI Payment Method ID', 'Last Modified When']
GOLD = [n.lower().replace(' ', '_') for n in NAMES[:11]]
TYPES = ['DATE'] + ['INT'] * 4 + ['STRING'] + ['DECIMAL(18,2)'] * 4 + ['INT'] * 6 + ['TIMESTAMP_NTZ']
EXPECTATIONS = {
    'one_source_id': '(`WWI Customer Transaction ID` IS NULL) <> (`WWI Supplier Transaction ID` IS NULL)',
    'source_kind': "(_source_kind = 'customer' AND `WWI Customer Transaction ID` IS NOT NULL) OR (_source_kind = 'supplier' AND `WWI Supplier Transaction ID` IS NOT NULL)",
    'roles': "(_source_kind = 'customer' AND `WWI Supplier ID` IS NULL AND `WWI Purchase Order ID` IS NULL AND `Supplier Invoice Number` IS NULL AND `WWI Customer ID` IS NOT NULL AND `WWI Bill To Customer ID` IS NOT NULL) OR (_source_kind = 'supplier' AND `WWI Customer ID` IS NULL AND `WWI Bill To Customer ID` IS NULL AND `WWI Invoice ID` IS NULL AND `WWI Supplier ID` IS NOT NULL)",
    'totals': '`Total Excluding Tax` + `Tax Amount` = `Total Including Tax`',
    'balance': '`Outstanding Balance` IS NOT NULL',
    'finalized': '`Is Finalized` IN (0, 1)',
    'event': '`Date Key` IS NOT NULL AND `Last Modified When` IS NOT NULL AND `WWI Transaction Type ID` IS NOT NULL',
}


def cutoff(value):
    if isinstance(value, list):
        return next(r['Cutoff Time'] for r in value if r['Table Name'] == 'Transaction')
    return value.ljust(27, '0') if len(value) > 19 else value + '.0000000'


def window(drop):
    return ('2012-12-31 00:00:00.0000000' if drop['name'] == 'D0'
            else cutoff(drop['last_cutoff']), cutoff(drop['new_cutoff']))


def extract_sql(bronze, drops):
    queries = []
    for drop in drops:
        d = drop['name']
        lo, hi = window(drop)
        ctes = []
        for source, table in SOURCES.items():
            pk = {'Sales.CustomerTransactions': 'CustomerTransactionID',
                  'Purchasing.SupplierTransactions': 'SupplierTransactionID',
                  'Sales.Invoices': 'InvoiceID'}[source]
            ctes.append(f"{table} AS (SELECT * EXCEPT (_rn) FROM (SELECT *, row_number() OVER (PARTITION BY {pk} ORDER BY _drop DESC, CASE WHEN _op = 'upsert' THEN 1 ELSE 0 END DESC, _row_ordinal DESC) _rn FROM {bronze}.{table}_raw WHERE _drop <= '{d}') WHERE _rn = 1 AND _op <> 'delete')")
        halves = []
        for kind, alias, table in [('customer', 'ct', 'sales_customer_transactions'), ('supplier', 'st', 'purchasing_supplier_transactions')]:
            customer = kind == 'customer'
            expressions = [f'{alias}.TransactionDate', 'ct.CustomerTransactionID' if customer else 'NULL',
                           'NULL' if customer else 'st.SupplierTransactionID', 'ct.InvoiceID' if customer else 'NULL',
                           'NULL' if customer else 'st.PurchaseOrderID', 'NULL' if customer else 'st.SupplierInvoiceNumber']
            expressions += [f'{alias}.{c}' for c in ['AmountExcludingTax', 'TaxAmount', 'TransactionAmount', 'OutstandingBalance', 'IsFinalized']]
            expressions += (['coalesce(i.CustomerID, ct.CustomerID)', 'ct.CustomerID', 'NULL'] if customer else ['NULL', 'NULL', 'st.SupplierID'])
            expressions += [f'{alias}.{c}' for c in ['TransactionTypeID', 'PaymentMethodID', 'LastEditedWhen']]
            projection = ', '.join(f'try_cast({e} AS {t}) AS `{n}`' for e, t, n in zip(expressions, TYPES, NAMES))
            join = ' LEFT JOIN sales_invoices i ON ct.InvoiceID = i.InvoiceID' if customer else ''
            halves.append(f"SELECT {projection}, '{d}' _drop, '{kind}' _source_kind FROM {table} {alias}{join} WHERE {alias}.LastEditedWhen > '{lo}' AND {alias}.LastEditedWhen <= '{hi}'")
        queries.append('(WITH ' + ', '.join(ctes) + ' ' + ' UNION ALL '.join(halves) + ')')
    return ' UNION ALL '.join(queries)


def read_csv(path):
    with open(path, newline='', encoding='utf-8-sig') as stream:
        return [{k: None if v == r'\N' else v for k, v in r.items()} for r in csv.DictReader(stream)]


def replay(state, upserts, deletes, pk):
    for row in deletes:
        del state[row[pk]]
    for row in upserts:
        state[row[pk]] = row


def extract_python(states, drop):
    lo, hi = window(drop)
    invoices = states['Sales.Invoices']
    for source, kind in [('Sales.CustomerTransactions', 'customer'), ('Purchasing.SupplierTransactions', 'supplier')]:
        for row in states[source].values():
            if not lo < row['LastEditedWhen'] <= hi:
                continue
            customer = kind == 'customer'
            values = [row['TransactionDate'][:10], row['CustomerTransactionID'] if customer else None,
                      None if customer else row['SupplierTransactionID'], row['InvoiceID'] if customer else None,
                      None if customer else row['PurchaseOrderID'], None if customer else row['SupplierInvoiceNumber']]
            values += [row[c] for c in ['AmountExcludingTax', 'TaxAmount', 'TransactionAmount', 'OutstandingBalance', 'IsFinalized']]
            invoice = invoices.get(row.get('InvoiceID'), {})
            values += ([invoice.get('CustomerID') or row['CustomerID'], row['CustomerID'], None] if customer else [None, None, row['SupplierID']])
            values += [row[c] for c in ['TransactionTypeID', 'PaymentMethodID', 'LastEditedWhen']]
            yield dict(zip(NAMES, values))


def project_python(rows):
    return Counter(tuple(r.get(n) for n in NAMES[:11]) for r in rows)
