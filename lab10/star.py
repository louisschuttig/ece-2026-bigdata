"""Lab 10.3: the gold layer as a star schema.

Run after lab10/trino_session.py:
    >>> exec(open("lab10/star.py").read())
The tables are created only if they do not exist.
"""
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_user AS
    SELECT
        row_number() OVER (ORDER BY user_id) AS user_key,
        user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users
""")
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)
""")

first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day,
           CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders
""").iloc[0]
sql(f"""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_date AS
    SELECT
        CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
        d                                       AS full_date,
        year(d)                                 AS year,
        quarter(d)                              AS quarter,
        month(d)                                AS month,
        date_format(CAST(d AS timestamp), '%M') AS month_name,
        day_of_week(d)                          AS day_of_week,
        day_of_week(d) IN (6, 7)                AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)
""")

sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.fact_orders AS
    SELECT
        o.order_id,
        u.user_key,
        p.product_key,
        CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
        o.ordered_at,
        o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product
""")

show("Row counts", """
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders
""")
print(f"dim_date covers {first_day} to {last_day}\n")
show("dim_date, first rows", "SELECT * FROM lakehouse.gold.dim_date ORDER BY date_key LIMIT 5")

# Query the star
show("Star query: year, state, product", """
    SELECT d.year, u.state, p.product,
           count(*)        AS orders,
           sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product
    ORDER BY units DESC
    LIMIT 10
""")
show("orders_by_state_month rebuilt from the star", """
    SELECT u.state, d.year, d.month,
           count(*)                   AS orders,
           sum(f.quantity)            AS units,
           count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month
    ORDER BY d.year, d.month, u.state
    LIMIT 10
""")
show("orders_by_state_month, same order", """
    SELECT state, year(month) AS year, month(month) AS month, orders, units, buyers
    FROM lakehouse.gold.orders_by_state_month
    ORDER BY 2, 3, 1
    LIMIT 10
""")

# Validation of the instructions
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
assert silver_orders == fact_orders, f"{silver_orders} != {fact_orders}"
assert silver_units == fact_units, f"{silver_units} != {fact_units}"
unmatched = sql("""
    SELECT count(*) AS v
    FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL
""").iloc[0, 0]
assert unmatched == 0, f"{unmatched} orders without a complete dimension row"
print(f"Validation passed: {fact_orders} orders, {fact_units} units, {unmatched} unmatched\n")

# Additional checks: defects that the validation above does not detect
show("Additional checks", """
    SELECT
      (SELECT count(*) - count(DISTINCT order_id) FROM lakehouse.gold.fact_orders) AS duplicated_order_ids,
      (SELECT count(*) - count(DISTINCT user_id) FROM lakehouse.gold.dim_user) AS duplicated_user_ids,
      (SELECT count(*) - count(DISTINCT product) FROM lakehouse.gold.dim_product) AS duplicated_products,
      (SELECT count(*)
         FROM lakehouse.gold.fact_orders AS f
         JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
        WHERE d.full_date <> CAST(f.ordered_at AS date)) AS wrong_dates,
      (SELECT count(*)
         FROM (SELECT p.product, sum(f.quantity) AS units
                 FROM lakehouse.gold.fact_orders AS f
                 JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
                GROUP BY p.product) AS g
         JOIN (SELECT product, sum(quantity) AS units FROM lakehouse.silver.orders GROUP BY product) AS s
           ON s.product = g.product
        WHERE g.units <> s.units) AS products_with_wrong_units
""")