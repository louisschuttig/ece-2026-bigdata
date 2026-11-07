"""Lab 10.2: the silver layer through Trino: catalogs, types, counts, Iceberg metadata, plans.

Run after lab10/trino_session.py:
    >>> exec(open("lab10/silver.py").read())
"""
show("Catalogs", "SHOW CATALOGS")
show("Schemas of lakehouse", "SHOW SCHEMAS FROM lakehouse")
show("Tables of lakehouse.silver", "SHOW TABLES FROM lakehouse.silver")
show("DESCRIBE silver.orders", "DESCRIBE lakehouse.silver.orders")
show("DESCRIBE silver.users", "DESCRIBE lakehouse.silver.users")

show("Orders", """
    SELECT count(*) AS orders, count(DISTINCT order_id) AS distinct_order_ids, sum(quantity) AS units
    FROM lakehouse.silver.orders
""")
show("Users", "SELECT count(*) AS users FROM lakehouse.silver.users")
show("Address parsing", """
    SELECT state,
           count(*) AS users,
           count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users
    GROUP BY state
    ORDER BY users DESC
    LIMIT 10
""")
show("Orphan orders", """
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL
""")

# Iceberg metadata, read through the hidden tables of the connector
show("orders$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.silver."orders$snapshots"
    ORDER BY committed_at
""")
show("orders$files", """
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lakehouse.silver."orders$files"
""")
show("orders$history", 'SELECT * FROM lakehouse.silver."orders$history"')

# Query plan and execution statistics
JOIN_QUERY = """
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state
"""
print("=== EXPLAIN (TYPE DISTRIBUTED) ===")
print(sql("EXPLAIN (TYPE DISTRIBUTED) " + JOIN_QUERY).iloc[0, 0])
print("=== EXPLAIN ANALYZE ===")
print(sql("EXPLAIN ANALYZE " + JOIN_QUERY).iloc[0, 0])
show("Last queries", """
    SELECT query_id, state, date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms,
           substr(query, 1, 60) AS query
    FROM system.runtime.queries
    ORDER BY created DESC
    LIMIT 5
""")