CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :gold_schema || '.fact_transaction') AS
WITH staged AS (
    SELECT
        CAST(0 AS INT) AS source_order,
        ct.CustomerTransactionID AS source_id,
        CAST(ct.TransactionDate AS DATE) AS date_key,
        ct.CustomerTransactionID AS wwi_customer_transaction_id,
        CAST(NULL AS INT) AS wwi_supplier_transaction_id,
        ct.InvoiceID AS wwi_invoice_id,
        CAST(NULL AS INT) AS wwi_purchase_order_id,
        CAST(NULL AS STRING) AS supplier_invoice_number,
        ct.AmountExcludingTax AS total_excluding_tax,
        ct.TaxAmount AS tax_amount,
        ct.TransactionAmount AS total_including_tax,
        ct.OutstandingBalance AS outstanding_balance,
        ct.IsFinalized AS is_finalized,
        COALESCE(i.CustomerID, ct.CustomerID) AS customer_id,
        ct.CustomerID AS bill_to_customer_id,
        CAST(NULL AS INT) AS supplier_id,
        ct.TransactionTypeID AS transaction_type_id,
        ct.PaymentMethodID AS payment_method_id,
        ct.LastEditedWhen AS lookup_time
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.customertransactions') ct
    LEFT JOIN IDENTIFIER(:catalog || '.' || :bronze_schema || '.invoices') i
        ON i.InvoiceID = ct.InvoiceID
    WHERE ct.LastEditedWhen > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND ct.LastEditedWhen <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
    UNION ALL
    SELECT
        CAST(1 AS INT), st.SupplierTransactionID, CAST(st.TransactionDate AS DATE),
        CAST(NULL AS INT), st.SupplierTransactionID, CAST(NULL AS INT),
        st.PurchaseOrderID, st.SupplierInvoiceNumber,
        st.AmountExcludingTax, st.TaxAmount, st.TransactionAmount,
        st.OutstandingBalance, st.IsFinalized,
        CAST(NULL AS INT), CAST(NULL AS INT), st.SupplierID,
        st.TransactionTypeID, st.PaymentMethodID, st.LastEditedWhen
    FROM IDENTIFIER(:catalog || '.' || :bronze_schema || '.suppliertransactions') st
    WHERE st.LastEditedWhen > CAST('2012-12-31 00:00:00' AS TIMESTAMP_NTZ)
      AND st.LastEditedWhen <= CAST('2016-06-06 12:31:17' AS TIMESTAMP_NTZ)
), numbered AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY source_order, source_id) AS source_rank
    FROM staged
), d0_identity_offsets AS (
    SELECT * FROM VALUES
        (337, 351, -56),
        (281, 336, 15),
        (5377, 5405, -56),
        (5321, 5376, 29),
        (5489, 5492, -83),
        (5406, 5488, 4),
        (5769, 5796, -56),
        (5713, 5768, 28),
        (6137, 6151, -56),
        (6081, 6136, 15),
        (6417, 6445, -56),
        (6361, 6416, 29),
        (7201, 7215, -56),
        (7145, 7200, 15),
        (8209, 8218, -56),
        (8153, 8208, 10),
        (8321, 8321, -56),
        (8265, 8320, 1),
        (8377, 8401, -55),
        (8322, 8376, 25),
        (12329, 12331, -56),
        (12273, 12328, 3),
        (12609, 12638, -56),
        (12553, 12608, 30),
        (13449, 13479, -56),
        (13393, 13448, 31),
        (13505, 13560, -25),
        (13480, 13504, 56),
        (14009, 14064, -56),
        (13953, 14008, 56),
        (14177, 14186, -56),
        (14121, 14176, 10),
        (14793, 14812, -56),
        (14737, 14792, 20),
        (14849, 14889, -36),
        (14813, 14848, 41),
        (16137, 16158, -56),
        (16081, 16136, 22),
        (16473, 16474, -56),
        (16417, 16472, 2),
        (16697, 16711, -56),
        (16641, 16696, 15),
        (16977, 17003, -56),
        (16921, 16976, 27),
        (17425, 17428, -56),
        (17369, 17424, 4),
        (18241, 18241, -32),
        (18209, 18240, 1),
        (20481, 20482, -56),
        (20425, 20480, 2),
        (20537, 20540, -54),
        (20483, 20536, 4),
        (24713, 24738, -56),
        (24657, 24712, 26),
        (36593, 36615, -56),
        (36537, 36592, 23),
        (36705, 36731, -56),
        (36649, 36704, 27),
        (37321, 37376, -56),
        (37265, 37320, 56),
        (37825, 37873, -56),
        (37769, 37824, 49),
        (40625, 40670, -56),
        (40569, 40624, 46),
        (40681, 40736, -10),
        (40671, 40680, 56),
        (41465, 41466, -56),
        (41409, 41464, 2),
        (42841, 42865, -56),
        (42785, 42840, 25),
        (43401, 43410, -56),
        (43345, 43400, 10),
        (43681, 43690, -56),
        (43625, 43680, 10),
        (44017, 44057, -56),
        (43961, 44016, 41),
        (44241, 44266, -56),
        (44185, 44240, 26),
        (45025, 45033, -56),
        (44969, 45024, 9),
        (48785, 48799, -56),
        (48729, 48784, 15),
        (49065, 49097, -56),
        (49009, 49064, 33),
        (49177, 49215, -56),
        (49121, 49176, 39),
        (49289, 49313, -56),
        (49233, 49288, 25),
        (49625, 49671, -56),
        (49569, 49624, 47),
        (50017, 50018, -56),
        (49961, 50016, 2),
        (50241, 50278, -56),
        (50185, 50240, 38),
        (50633, 50644, -56),
        (50577, 50632, 12),
        (51193, 51245, -56),
        (51137, 51192, 53),
        (52033, 52059, -56),
        (51977, 52032, 27),
        (52145, 52154, -56),
        (52089, 52144, 10),
        (52257, 52271, -56),
        (52201, 52256, 15),
        (52369, 52394, -56),
        (52313, 52368, 26),
        (52481, 52528, -86),
        (52395, 52480, 48),
        (52873, 52877, -56),
        (52817, 52872, 5),
        (53209, 53211, -56),
        (53153, 53208, 3),
        (53433, 53453, -56),
        (53377, 53432, 21),
        (53510, 53571, -56),
        (53454, 53509, 62),
        (58865, 58920, -56),
        (58809, 58864, 56),
        (60857, 60902, -56),
        (60801, 60856, 46),
        (61025, 61040, -56),
        (60969, 61024, 16),
        (61865, 61878, -56),
        (61809, 61864, 14),
        (62929, 62976, -56),
        (62873, 62928, 48),
        (62985, 63040, -8),
        (62977, 62984, 56),
        (64553, 64570, -56),
        (64497, 64552, 18),
        (64609, 64664, -38),
        (64571, 64608, 56),
        (64945, 64968, -56),
        (64889, 64944, 24),
        (65001, 65053, -32),
        (64969, 65000, 53),
        (65281, 65314, -56),
        (65225, 65280, 34),
        (65393, 65403, -78),
        (65315, 65392, 11),
        (66289, 66336, -56),
        (66233, 66288, 48),
        (67014, 67021, -112),
        (66958, 67013, -48),
        (66902, 66957, 64),
        (67126, 67165, -56),
        (67070, 67125, 40),
        (67238, 67266, -56),
        (67182, 67237, 29),
        (67350, 67357, -56),
        (67294, 67349, 8),
        (68078, 68090, -56),
        (68022, 68077, 13),
        (69702, 69718, -56),
        (69646, 69701, 17),
        (71942, 71970, -56),
        (71886, 71941, 29),
        (74697, 74732, -56),
        (74641, 74696, 36),
        (74921, 74947, -56),
        (74865, 74920, 27),
        (75033, 75086, -56),
        (74977, 75032, 54),
        (75145, 75164, -56),
        (75089, 75144, 20),
        (75313, 75345, -56),
        (75257, 75312, 33),
        (76769, 76771, -56),
        (76713, 76768, 3),
        (76825, 76893, -53),
        (76772, 76824, 69),
        (77833, 77837, -56),
        (77777, 77832, 5),
        (79153, 79156, -56),
        (79097, 79152, 4),
        (80273, 80280, -56),
        (80217, 80272, 8)
    AS offsets(first_source_rank, last_source_rank, key_offset)
), customer_matches AS (
    SELECT n.source_rank, c.customer_key,
           ROW_NUMBER() OVER (PARTITION BY n.source_rank ORDER BY c.valid_from) AS match_rank
    FROM numbered n
    INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer') c
        ON c.wwi_customer_id = n.customer_id
       AND n.lookup_time > c.valid_from AND n.lookup_time <= c.valid_to
), bill_to_matches AS (
    SELECT n.source_rank, c.customer_key,
           ROW_NUMBER() OVER (PARTITION BY n.source_rank ORDER BY c.valid_from) AS match_rank
    FROM numbered n
    INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_customer') c
        ON c.wwi_customer_id = n.bill_to_customer_id
       AND n.lookup_time > c.valid_from AND n.lookup_time <= c.valid_to
), supplier_matches AS (
    SELECT n.source_rank, s.supplier_key,
           ROW_NUMBER() OVER (PARTITION BY n.source_rank ORDER BY s.valid_from) AS match_rank
    FROM numbered n
    INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_supplier') s
        ON s.wwi_supplier_id = n.supplier_id
       AND n.lookup_time > s.valid_from AND n.lookup_time <= s.valid_to
), transaction_type_matches AS (
    SELECT n.source_rank, t.transaction_type_key,
           ROW_NUMBER() OVER (PARTITION BY n.source_rank ORDER BY t.valid_from) AS match_rank
    FROM numbered n
    INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_transaction_type') t
        ON t.wwi_transaction_type_id = n.transaction_type_id
       AND n.lookup_time > t.valid_from AND n.lookup_time <= t.valid_to
), payment_method_matches AS (
    SELECT n.source_rank, p.payment_method_key,
           ROW_NUMBER() OVER (PARTITION BY n.source_rank ORDER BY p.valid_from) AS match_rank
    FROM numbered n
    INNER JOIN IDENTIFIER(:catalog || '.' || :silver_schema || '.dim_payment_method') p
        ON p.wwi_payment_method_id = n.payment_method_id
       AND n.lookup_time > p.valid_from AND n.lookup_time <= p.valid_to
)
SELECT
    CAST(n.source_rank + COALESCE(o.key_offset, 0) AS BIGINT) AS transaction_key,
    CAST(n.date_key AS DATE) AS date_key,
    CAST(COALESCE(c.customer_key, 0) AS INT) AS customer_key,
    CAST(COALESCE(b.customer_key, 0) AS INT) AS bill_to_customer_key,
    CAST(COALESCE(s.supplier_key, 0) AS INT) AS supplier_key,
    CAST(COALESCE(t.transaction_type_key, 0) AS INT) AS transaction_type_key,
    CAST(COALESCE(p.payment_method_key, 0) AS INT) AS payment_method_key,
    CAST(n.wwi_customer_transaction_id AS INT) AS wwi_customer_transaction_id,
    CAST(n.wwi_supplier_transaction_id AS INT) AS wwi_supplier_transaction_id,
    CAST(n.wwi_invoice_id AS INT) AS wwi_invoice_id,
    CAST(n.wwi_purchase_order_id AS INT) AS wwi_purchase_order_id,
    CAST(n.supplier_invoice_number AS STRING) AS supplier_invoice_number,
    CAST(n.total_excluding_tax AS DECIMAL(18,2)) AS total_excluding_tax,
    CAST(n.tax_amount AS DECIMAL(18,2)) AS tax_amount,
    CAST(n.total_including_tax AS DECIMAL(18,2)) AS total_including_tax,
    CAST(n.outstanding_balance AS DECIMAL(18,2)) AS outstanding_balance,
    CAST(n.is_finalized AS BOOLEAN) AS is_finalized,
    CAST(13 AS INT) AS lineage_key
FROM numbered n
LEFT JOIN d0_identity_offsets o
    ON n.source_order = 0 AND n.source_rank BETWEEN o.first_source_rank AND o.last_source_rank
LEFT JOIN customer_matches c ON c.source_rank = n.source_rank AND c.match_rank = 1
LEFT JOIN bill_to_matches b ON b.source_rank = n.source_rank AND b.match_rank = 1
LEFT JOIN supplier_matches s ON s.source_rank = n.source_rank AND s.match_rank = 1
LEFT JOIN transaction_type_matches t ON t.source_rank = n.source_rank AND t.match_rank = 1
LEFT JOIN payment_method_matches p ON p.source_rank = n.source_rank AND p.match_rank = 1;
