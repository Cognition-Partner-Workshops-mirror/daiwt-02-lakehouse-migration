from pyspark import pipelines as dp
from pyspark.sql import functions as F

catalog = spark.conf.get('payment_method.catalog')
bronze = spark.conf.get('payment_method.bronze_schema')
quality = {
    'business_key': 'wwi_payment_method_id IS NOT NULL AND wwi_payment_method_id > 0',
    'name': 'payment_method IS NOT NULL AND length(payment_method) BETWEEN 1 AND 50',
    'validity': 'valid_from IS NOT NULL AND valid_to IS NOT NULL AND valid_from < valid_to',
}


@dp.materialized_view(name='payment_method_changes')
@dp.expect_all(quality)
def payment_method_changes():
    return spark.sql(f"""
        WITH history AS (
            SELECT PaymentMethodID, PaymentMethodName, ValidFrom, ValidTo
            FROM {catalog}.{bronze}.application_payment_methods
            UNION ALL
            SELECT PaymentMethodID, PaymentMethodName, ValidFrom, ValidTo
            FROM {catalog}.{bronze}.application_payment_methods_archive
        ), events AS (
            SELECT h.PaymentMethodID, h.ValidFrom
            FROM history h CROSS JOIN {catalog}.{bronze}.payment_method_window w
            WHERE h.ValidFrom > w.last_cutoff AND h.ValidFrom <= w.new_cutoff
        ), staged AS (
            SELECT h.PaymentMethodID AS wwi_payment_method_id,
                   h.PaymentMethodName AS payment_method, h.ValidFrom AS valid_from
            FROM events e JOIN history h ON e.PaymentMethodID = h.PaymentMethodID
             AND h.ValidFrom <= e.ValidFrom AND e.ValidFrom < h.ValidTo
        )
        SELECT s.wwi_payment_method_id, s.payment_method, s.valid_from,
               COALESCE((SELECT MIN(t.valid_from) FROM staged t
                         WHERE t.wwi_payment_method_id = s.wwi_payment_method_id
                         AND t.valid_from > s.valid_from),
                        '9999-12-31 23:59:59.9999999') AS valid_to
        FROM staged s
    """)


@dp.materialized_view(name='payment_method_changes_quarantine')
def payment_method_changes_quarantine():
    valid = ' AND '.join(f'({rule})' for rule in quality.values())
    return spark.read.table('payment_method_changes').filter(~F.coalesce(F.expr(valid), F.lit(False)))
