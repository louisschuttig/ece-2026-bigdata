"""Lab 7, silver layer: orders, deduplication and Spark SQL.

Run after lab7/silver_lab.py, in the same PySpark shell:
    >>> exec(open("lab7/orders.py").read())
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())

# Intentionally create duplicates, as a replayed ingestion would
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show(truncate=False)

# Deduplication with a window function, like qualify row_number() in the dbt lab
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

# The same transformation with Spark SQL
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("DataFrame and SQL versions agree:", stg_orders_sql.count(), "orders")

# Analytical query on the silver users, in SQL
stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
""").show(10)