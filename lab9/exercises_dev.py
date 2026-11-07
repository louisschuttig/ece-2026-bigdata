"""Lab 9, exercises as dev: drop the country column, create a gold namespace and table.

Run after lab9/session.py started with POLARIS_PRINCIPAL=dev:
    >>> exec(open("lab9/exercises_dev.py").read())
"""
assert POLARIS_PRINCIPAL == "dev", "restart the session with POLARIS_PRINCIPAL=dev"
USERS = "polaris.silver.users"

# The failed INSERT of the analyst, checked as dev
print("=== Snapshots of polaris.silver.orders ===")
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM polaris.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)

# Exercise 1: remove the country column
print("=== Exercise 1: drop the country column ===")
if "country" in spark.table(USERS).columns:
    spark.sql(f"ALTER TABLE {USERS} DROP COLUMN country")
spark.sql(f"DESCRIBE {USERS}").show()
spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {USERS}.snapshots ORDER BY committed_at").show(truncate=False)
spark.sql(f"""
    SELECT timestamp, file, latest_snapshot_id
    FROM {USERS}.metadata_log_entries
    ORDER BY timestamp
""").show(truncate=False)
print("Rows:", spark.table(USERS).count())

# Exercise 2: a gold namespace with one table, created by dev
print("=== Exercise 2: gold namespace and table ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
if "orders_per_product" not in [row.tableName for row in spark.sql("SHOW TABLES IN polaris.gold").collect()]:
    spark.sql("""
        CREATE TABLE polaris.gold.orders_per_product AS
        SELECT product, count(*) AS orders, sum(quantity) AS quantity
        FROM polaris.silver.orders
        GROUP BY product
    """)
spark.sql("SELECT * FROM polaris.gold.orders_per_product ORDER BY product").show()
spark.sql("SHOW NAMESPACES IN polaris").show()