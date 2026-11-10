"""Customer temporal SQL and golden replay contract (datetime2(7) stays textual)."""
import re

SOURCES = ("Sales.Customers", "Sales.CustomerCategories", "Sales.BuyingGroups", "Application.People")
TABLES = tuple(source + suffix for source in SOURCES for suffix in ("", "_Archive"))
COLUMNS = ("wwi_customer_id", "customer", "bill_to_customer", "category", "buying_group",
           "primary_contact", "postal_code", "valid_from", "valid_to")
END = "9999-12-31 23:59:59.9999999"


def datetime7(value):
    if not re.fullmatch(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d{1,7})?", value):
        raise ValueError(f"Invalid datetime2(7): {value!r}")
    whole, _, fraction = value.partition(".")
    return whole + "." + fraction.ljust(7, "0")


def cutoffs(manifest, drop):
    entry = next((d for d in manifest["drops"] if d["name"] == drop), None)
    if entry is None:
        raise ValueError(f"Unknown drop: {drop}")
    def customer(value):
        if isinstance(value, list):
            value = next(r["Cutoff Time"] for r in value if r["Table Name"] == "Customer")
        return datetime7(value)
    return customer(entry["last_cutoff"]), customer(entry["new_cutoff"])


def table_name(source):
    schema, table = source.split(".")
    return schema.lower() + "_" + re.sub(r"(?<=[a-z])(?=[A-Z])", "_", table).lower()


def window_sql(catalog, bronze_schema):
    cutoffs = ", ".join(
        f"concat(split_part({column}, '.', 1), '.', rpad(split_part({column}, '.', 2), 7, '0')) AS {column}"
        for column in ("last_cutoff", "new_cutoff"))
    return (f"SELECT drop, {cutoffs} FROM `{catalog}`.`{bronze_schema}`.golden_drops "
            f"WHERE table_name = 'Customer' AND drop = (SELECT max_by(drop, drop_index) "
            f"FROM `{catalog}`.`{bronze_schema}`.bronze_replay_log WHERE status = 'PASS')")


def history_sql(catalog, bronze_schema):
    history = []
    for source in SOURCES:
        name = table_name(source)
        alias = source.replace(".", "_").lower()
        projection = "* EXCEPT (ValidFrom, ValidTo), ValidFrom_str AS ValidFrom, ValidTo_str AS ValidTo"
        history.append(f"{alias} AS (SELECT {projection} FROM `{catalog}`.`{bronze_schema}`.{name} "
                       f"UNION ALL SELECT {projection} FROM `{catalog}`.`{bronze_schema}`.{name}_archive)")
    return history


def extract_sql(catalog, bronze_schema, last_cutoff=None, new_cutoff=None,
                initial_load_date="2020-01-01"):
    for name in (catalog, bronze_schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Invalid catalog/schema")
    initial = datetime7(initial_load_date + " 00:00:00")
    history = []
    if last_cutoff is None and new_cutoff is None:
        history.append(f"run_window AS ({window_sql(catalog, bronze_schema)})")
        window = "ValidFrom > (SELECT last_cutoff FROM run_window) AND ValidFrom <= (SELECT new_cutoff FROM run_window)"
    else:
        last, new = datetime7(last_cutoff), datetime7(new_cutoff)
        if last > new:
            raise ValueError("last_cutoff exceeds new_cutoff")
        window = f"ValidFrom > '{last}' AND ValidFrom <= '{new}'"
    history.extend(history_sql(catalog, bronze_schema))
    # SQL Server chooses the next strictly greater ValidFrom, even with duplicate events.
    prefix = "WITH " + ",\n".join(history)
    events = []
    for kind, table, key in (("customer", "sales_customers", "CustomerID"),
                             ("category", "sales_customercategories", "CustomerCategoryID"),
                             ("group", "sales_buyinggroups", "BuyingGroupID")):
        exclude = "" if kind == "customer" else f" AND ValidFrom <> '{initial}'"
        events.append(f"SELECT '{kind}' kind, {key} id, ValidFrom t FROM {table} WHERE {window}{exclude}")
    return prefix + ", events AS (" + " UNION ALL ".join(events) + """), changes AS (
 SELECT CAST(c.CustomerID AS INT) wwi_customer_id, c.CustomerName customer,
 bt.CustomerName bill_to_customer, cc.CustomerCategoryName category,
 bg.BuyingGroupName buying_group, p.FullName primary_contact,
 c.DeliveryPostalCode postal_code, c.ValidFrom valid_from
 FROM events e
 JOIN sales_customers c ON c.ValidFrom <= e.t AND e.t < c.ValidTo
 AND ((e.kind = 'customer' AND c.CustomerID = e.id)
   OR (e.kind = 'category' AND c.CustomerCategoryID = e.id)
   OR (e.kind = 'group' AND c.BuyingGroupID = e.id))
 JOIN sales_customercategories cc ON c.CustomerCategoryID = cc.CustomerCategoryID
 AND cc.ValidFrom <= e.t AND e.t < cc.ValidTo
 JOIN sales_customers bt ON c.BillToCustomerID = bt.CustomerID
 AND bt.ValidFrom <= e.t AND e.t < bt.ValidTo
 JOIN application_people p ON c.PrimaryContactPersonID = p.PersonID
 AND p.ValidFrom <= e.t AND e.t < p.ValidTo
 LEFT JOIN sales_buyinggroups bg ON c.BuyingGroupID = bg.BuyingGroupID
 AND bg.ValidFrom <= e.t AND e.t < bg.ValidTo
), times AS (
 SELECT wwi_customer_id, valid_from,
 LEAD(valid_from) OVER (PARTITION BY wwi_customer_id ORDER BY valid_from) next_from
 FROM (SELECT DISTINCT wwi_customer_id, valid_from FROM changes)
)
SELECT """ + ", ".join("c." + col for col in COLUMNS[:-1]) + f""",
 COALESCE(t.next_from, '{END}') valid_to
FROM changes c JOIN times t
 ON c.wwi_customer_id = t.wwi_customer_id AND c.valid_from = t.valid_from
WHERE c.buying_group IS NOT NULL
ORDER BY c.valid_from"""
