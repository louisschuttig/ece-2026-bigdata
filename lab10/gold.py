"""Lab 10.2: build, validate and refresh the gold layer with Trino.

Run after lab10/trino_session.py:
    >>> exec(open("lab10/gold.py").read())
The tables are created only if they do not exist, and top_products is refreshed only once.
"""
sql("CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
show("Schemas of lakehouse", "SHOW SCHEMAS FROM lakehouse")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_by_state_month AS
    SELECT
        u.state,
        date_trunc('month', o.ordered_at) AS month,
        count(*)                          AS orders,
        sum(o.quantity)                   AS units,
        count(DISTINCT o.user_id)         AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")
show("gold.orders_by_state_month", "SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.top_products AS
    SELECT
        product,
        count(*)      AS orders,
        sum(quantity) AS units,
        rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product
""")
show("gold.top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.user_activity AS
    SELECT
        u.user_id,
        u.state,
        date_diff('year', u.birthdate, current_date) AS age,
        count(o.order_id)            AS orders,
        coalesce(sum(o.quantity), 0) AS units,
        min(o.ordered_at)            AS first_order_at,
        max(o.ordered_at)            AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate
""")
show("gold.user_activity", "SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10")
show("Negative ages (future birthdates of the generator)", "SELECT count_if(age < 0) AS users FROM lakehouse.gold.user_activity")

show("Tables of lakehouse.gold", "SHOW TABLES FROM lakehouse.gold")
show("top_products$files", 'SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')

# Validation against the silver layer
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
assert silver_units == gold_units, f"{silver_units} != {gold_units}"
assert silver_units == product_units, f"{silver_units} != {product_units}"

silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
assert silver_users == gold_users, f"{silver_users} != {gold_users}"
assert silver_orders == gold_orders, f"{silver_orders} != {gold_orders}"
print(f"Validation passed: {silver_units} units, {silver_users} users, {silver_orders} orders\n")

# Refresh top_products: DELETE then INSERT, one snapshot each
if len(sql('SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots"')) == 1:
    sql("DELETE FROM lakehouse.gold.top_products")
    sql("""
        INSERT INTO lakehouse.gold.top_products
        SELECT product, count(*), sum(quantity),
               rank() OVER (ORDER BY sum(quantity) DESC)
        FROM lakehouse.silver.orders
        GROUP BY product
    """)
show("top_products$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
""")
first = sql("""
    SELECT snapshot_id
    FROM lakehouse.gold."top_products$snapshots"
    ORDER BY committed_at
    LIMIT 1
""").iloc[0, 0]
show(f"FOR VERSION AS OF {first}", f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}")
show("Current version", "SELECT count(*) AS products FROM lakehouse.gold.top_products")