"""Lab 9, cleanup as dev: drop the tables created in Polaris, keep silver.orders for the Trino lab.

Run after lab9/session.py started with POLARIS_PRINCIPAL=dev:
    >>> exec(open("lab9/cleanup.py").read())
"""
assert POLARIS_PRINCIPAL == "dev", "restart the session with POLARIS_PRINCIPAL=dev"

# A namespace must be empty before it is dropped
spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_per_product")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")

spark.sql("SHOW NAMESPACES IN polaris").show()
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
# DROP TABLE without PURGE only removes the catalog entry: the files of the shared directory are untouched
print("lab.silver.users, same directory, still readable:", spark.table("lab.silver.users").count(), "rows")