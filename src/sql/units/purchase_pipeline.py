"""Deterministic drop extracts and accumulated purchase staging."""
from pyspark import pipelines as dp
from purchase_core import SOURCES, cutoff, manifest
from purchase_history import completed_drop_sql, snapshot_sql

catalog = spark.conf.get('purchase.catalog')
bronze = spark.conf.get('purchase.bronze_schema')
spec = manifest(spark.conf.get('purchase.golden_path'))
rules = {
    'ids_not_null': 'purchase_order_line_id IS NOT NULL AND wwi_purchase_order_id IS NOT NULL '
                    'AND wwi_supplier_id IS NOT NULL AND wwi_stock_item_id IS NOT NULL',
    'quantity_conversion': 'ordered_quantity = ordered_outers * quantity_per_outer',
    'line_finalized': 'is_order_finalized IN (0, 1)',
}
valid = ' AND '.join(f'({rule})' for rule in rules.values())


def register_history(source, table):
    @dp.temporary_view(name='purchase_' + table)
    def history():
        return spark.sql(' UNION ALL '.join(
            snapshot_sql(catalog, bronze, source, drop, spec)
            for drop in ('D0', 'D1', 'D2')))


for source, table in SOURCES.items():
    register_history(source, table)

queries = []
for drop in spec['drops'][1:]:
    name = drop['name']
    modified = 'CASE WHEN pol.LastEditedWhen > po.LastEditedWhen THEN pol.LastEditedWhen ELSE po.LastEditedWhen END'
    # For missing parents, the line timestamp is the available window evidence.
    window_time = f'CASE WHEN po.PurchaseOrderID IS NULL THEN pol.LastEditedWhen ELSE {modified} END'
    queries.append(f"""
        SELECT CAST(po.OrderDate AS DATE) date_key,
               po.PurchaseOrderID wwi_purchase_order_id,
               pol.OrderedOuters ordered_outers,
               pol.OrderedOuters * si.QuantityPerOuter ordered_quantity,
               pol.ReceivedOuters received_outers, pt.PackageTypeName package,
               CAST(pol.IsOrderLineFinalized AS INT) is_order_finalized,
               po.SupplierID wwi_supplier_id, pol.StockItemID wwi_stock_item_id,
               {modified} last_modified_when, '{name}' _drop,
               pol.PurchaseOrderLineID purchase_order_line_id,
               row_number() OVER (ORDER BY pol.PurchaseOrderID, pol.PurchaseOrderLineID) _staged_order,
               si.QuantityPerOuter quantity_per_outer,
               (po.PurchaseOrderID IS NULL OR si.StockItemID IS NULL OR pt.PackageTypeID IS NULL) _orphan
        FROM purchase_purchasing_purchase_order_lines pol
        LEFT JOIN purchase_purchasing_purchase_orders po
          ON pol.PurchaseOrderID = po.PurchaseOrderID AND po._drop = '{name}'
        LEFT JOIN purchase_warehouse_stock_items si
          ON pol.StockItemID = si.StockItemID AND si._drop = '{name}'
        LEFT JOIN purchase_warehouse_package_types pt
          ON pol.PackageTypeID = pt.PackageTypeID AND pt._drop = '{name}'
        WHERE pol._drop = '{name}'
          AND ({completed_drop_sql(catalog, bronze, name)})
          AND {window_time} > '{cutoff(drop, 'last_cutoff')}'
          AND {window_time} <= '{cutoff(drop, 'new_cutoff')}'
    """)


@dp.temporary_view()
def purchase_candidates():
    return spark.sql(' UNION ALL '.join(queries))


@dp.materialized_view(private=True)
@dp.expect_all(rules)
def purchase_validated():
    return spark.sql('''SELECT * EXCEPT (_orphan, _staged_order, quantity_per_outer),
        row_number() OVER (PARTITION BY _drop ORDER BY wwi_purchase_order_id,
                          purchase_order_line_id) _staged_order, quantity_per_outer
        FROM purchase_candidates WHERE NOT _orphan''')


@dp.materialized_view()
def purchase_changes():
    return spark.sql('SELECT * EXCEPT (quantity_per_outer) FROM purchase_validated')


@dp.materialized_view()
def purchase_changes_quarantine():
    return spark.sql(f'SELECT * FROM purchase_candidates WHERE _orphan OR NOT coalesce(({valid}), false)')


@dp.materialized_view()
def purchase_staging():
    # Staging accumulates across drops, matching the legacy control flow.
    return spark.sql(f"""SELECT c.*, n._staging_drop FROM purchase_changes c
        CROSS JOIN (SELECT explode(array('D1', 'D2')) _staging_drop) n
        WHERE c._drop <= n._staging_drop AND n._staging_drop IN (
            SELECT drop FROM `{catalog}`.`{bronze}`.bronze_replay_log
            WHERE status = 'PASS' GROUP BY drop HAVING count(DISTINCT bronze_table) = 38)""")
