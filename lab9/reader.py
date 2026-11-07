"""Lab 9, exercise 2: test the principal reader, limited to the silver namespace.

Run after lab9/session.py started with POLARIS_PRINCIPAL=reader:
    >>> exec(open("lab9/reader.py").read())
"""
assert POLARIS_PRINCIPAL == "reader", "restart the session with POLARIS_PRINCIPAL=reader"


def try_query(label, statement):
    try:
        rows = spark.sql(statement).collect()
        print(f"{label}: OK -> {[tuple(row) for row in rows]}")
    except Exception as error:
        lines = str(error).splitlines()
        message = next((line for line in lines if "Forbidden" in line or "Exception" in line), lines[0])
        print(f"{label}: FAILED -> {message.strip()[:300]}")


try_query("SHOW TABLES IN polaris.silver", "SHOW TABLES IN polaris.silver")
try_query("count polaris.silver.orders", "SELECT count(*) FROM polaris.silver.orders")
try_query("count polaris.silver.users", "SELECT count(*) FROM polaris.silver.users")
try_query("SHOW TABLES IN polaris.gold", "SHOW TABLES IN polaris.gold")
try_query("count polaris.gold.orders_per_product", "SELECT count(*) FROM polaris.gold.orders_per_product")
try_query("INSERT INTO polaris.silver.orders", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")