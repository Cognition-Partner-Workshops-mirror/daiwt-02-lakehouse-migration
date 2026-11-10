# G16: Customer-history report

`wwi_report_customer_history_to_gold` owns only `gold.report_customer_history`
and depends on `wwi_dim_customer_to_gold`. Its `load` and `parity` tasks accept
the P2 drop parameters.

Every drop rebuilds the report from the current `gold.dim_customer` using the
capture's customer-history SQL:

```sql
SELECT [Customer Key], [WWI Customer ID], Customer, [Valid From], [Valid To]
FROM Dimension.Customer ORDER BY [Customer Key]
```

There is no filter, so unknown key 0 and every historical version are kept.
Columns are copied as stored in the dimension (BIGINT keys and IDs,
datetime2(7) validity strings), which the parity harness renders to the golden
CSV text. Parity is a multiset comparison, so file row order does not matter.

The per-drop `parity` task uses the shared row-for-row parity harness against
`reports/customer-history.csv` (403 rows on D0, D1 and D2). The unit tests run the
generated SQL over each drop's golden `Dimension.Customer.csv` and compare with
that drop's golden report.
