"""Lab 10.3, exercise 5: the aggregation of orders_by_state_month with Spark, through Polaris.

Run after lab9/session.py started with POLARIS_PRINCIPAL=dev:
    >>> exec(open("lab10/spark_compare.py").read())
"""
assert POLARIS_PRINCIPAL == "dev", "restart the session with POLARIS_PRINCIPAL=dev"
spark.conf.set("spark.sql.session.timeZone", "UTC")

spark_result = spark.sql("""
    SELECT u.state, date_trunc('MONTH', o.ordered_at) AS month,
           count(*) AS orders, sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
    FROM polaris.silver.orders AS o
    JOIN polaris.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2
""")
trino_result = spark.table("polaris.gold.orders_by_state_month").select("state", "month", "orders", "units", "buyers")

for label, df in [("Spark aggregation", spark_result), ("Trino gold table", trino_result)]:
    row = df.selectExpr("count(*) AS rows", "sum(orders) AS orders", "sum(units) AS units").first()
    print(f"{label}: {row['rows']} rows, {row['orders']} orders, {row['units']} units")
print("Rows only in the Spark result:", spark_result.exceptAll(trino_result).count())
print("Rows only in the Trino table:", trino_result.exceptAll(spark_result).count())