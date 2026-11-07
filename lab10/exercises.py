"""Lab 10.3, exercises 1 to 4 with Trino.

Run after lab10/star.py:
    >>> exec(open("lab10/exercises.py").read())
Each write is skipped if it has already been done.
"""


def scan_stats(statement):
    """Input rows and splits of each table scan of a query, from EXPLAIN ANALYZE."""
    plan = sql("EXPLAIN ANALYZE " + statement).iloc[0, 0]
    return [line.strip() for line in plan.splitlines() if "Physical input" in line]


def tables(schema):
    return set(sql(f"SHOW TABLES FROM lakehouse.{schema}")["Table"])


# Exercise 1: partitioned copy of orders_by_state_month
print("=== Exercise 1: partitioning ===")
if "orders_by_state_month_part" not in tables("gold"):
    sql("""
        CREATE TABLE lakehouse.gold.orders_by_state_month_part
        WITH (partitioning = ARRAY['month(month)'])
        AS SELECT * FROM lakehouse.gold.orders_by_state_month
    """)
for table in ["orders_by_state_month", "orders_by_state_month_part"]:
    show(f"{table}$files", f"""
        SELECT regexp_extract(file_path, '[^/]+/data/.*') AS file, record_count, file_size_in_bytes
        FROM lakehouse.gold."{table}$files"
    """)
show("orders_by_state_month_part$partitions", 'SELECT * FROM lakehouse.gold."orders_by_state_month_part$partitions"')
for table in ["orders_by_state_month", "orders_by_state_month_part"]:
    print(f"Filter on February 2020, {table}:")
    for line in scan_stats(f"""
        SELECT sum(units) FROM lakehouse.gold.{table}
        WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'
    """):
        print("   ", line)
print()

# Exercise 2: cohorts by decade of birth
print("=== Exercise 2: cohorts ===")
sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold.cohorts AS
    SELECT
        (year(u.birthdate) / 10) * 10           AS birth_decade,
        count(DISTINCT u.user_id)               AS users,
        count(o.order_id)                       AS orders,
        round(avg(CAST(o.quantity AS double)), 2) AS avg_units_per_order
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY 1
""")
show("gold.cohorts", "SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade")

# Exercise 3: append one order to silver, refresh the gold table with MERGE, time travel
print("=== Exercise 3: append, MERGE, time travel ===")
NEW_ORDER = "lab10-new-order-1"
user_id, state = sql("SELECT user_id, state FROM lakehouse.silver.users ORDER BY user_id LIMIT 1").iloc[0]
if sql(f"SELECT count(*) AS v FROM lakehouse.silver.orders WHERE order_id = '{NEW_ORDER}'").iloc[0, 0] == 0:
    sql(f"""
        INSERT INTO lakehouse.silver.orders (order_id, user_id, ordered_at, quantity, product, source)
        VALUES ('{NEW_ORDER}', '{user_id}', TIMESTAMP '2020-04-28 12:00:00 UTC', 2, 'croissant', 'trino')
    """)
show("silver.orders$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots"
    ORDER BY committed_at
""")

silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
if silver_units != gold_units:
    sql("""
        MERGE INTO lakehouse.gold.orders_by_state_month AS t
        USING (
            SELECT u.state, date_trunc('month', o.ordered_at) AS month,
                   count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
            FROM lakehouse.silver.orders AS o
            JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
            GROUP BY 1, 2
        ) AS s
        ON t.state = s.state AND t.month = s.month
        WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
            THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
        WHEN NOT MATCHED
            THEN INSERT (state, month, orders, units, buyers) VALUES (s.state, s.month, s.orders, s.units, s.buyers)
    """)
show("gold.orders_by_state_month$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."orders_by_state_month$snapshots"
    ORDER BY committed_at
""")
first = sql("""
    SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots"
    ORDER BY committed_at LIMIT 1
""").iloc[0, 0]
ROW_FILTER = f"WHERE state = '{state}' AND month = TIMESTAMP '2020-04-01 00:00:00 UTC'"
show(f"State {state}, April 2020, FOR VERSION AS OF {first}",
     f"SELECT * FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {first} {ROW_FILTER}")
show(f"State {state}, April 2020, current version",
     f"SELECT * FROM lakehouse.gold.orders_by_state_month {ROW_FILTER}")
show("Units: silver, gold before, gold now", f"""
    SELECT (SELECT sum(quantity) FROM lakehouse.silver.orders) AS silver_units,
           (SELECT sum(units) FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {first}) AS gold_units_before,
           (SELECT sum(units) FROM lakehouse.gold.orders_by_state_month) AS gold_units_now
""")

# Exercise 4: compaction with optimize
print("=== Exercise 4: optimize ===")
for table in ["top_products", "orders_by_state_month"]:
    for label in ["before", "after"]:
        if label == "after":
            sql(f"ALTER TABLE lakehouse.gold.{table} EXECUTE optimize")
        show(f"{table} snapshots, {label} optimize", f"""
            SELECT snapshot_id, operation, committed_at
            FROM lakehouse.gold."{table}$snapshots" ORDER BY committed_at
        """)
        show(f"{table} files, {label} optimize", f"""
            SELECT regexp_extract(file_path, '[^/]+$') AS file, content, record_count, file_size_in_bytes
            FROM lakehouse.gold."{table}$files"
        """)