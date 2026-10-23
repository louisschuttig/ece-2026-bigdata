"""Lab 7: write the silver layer as Parquet files and as Iceberg tables.

Run after lab7/silver_lab.py and lab7/orders.py, in the same PySpark shell:
    >>> exec(open("lab7/write.py").read())
"""
# Parquet: one directory of part-files per DataFrame
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")

users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("Parquet:", users_silver.count(), "users,", orders_silver.count(), "orders")

# Iceberg: the same data, plus metadata describing the table and its snapshots
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()

spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
""").show(truncate=False)
spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)

users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("Iceberg:", users_iceberg.count(), "users,", orders_iceberg.count(), "orders")