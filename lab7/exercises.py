"""Lab 7: exercises.

Run after lab7/silver_lab.py and lab7/orders.py, in the same PySpark shell
(the Iceberg tables are already written by lab7/write.py):
    >>> exec(open("lab7/exercises.py").read())
"""
import re
import time
import timeit

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, udf


def timed(label, action):
    start = time.perf_counter()
    result = action()
    print(f"{label}: {result} in {time.perf_counter() - start:.3f} s")


# Exercise 1: birthdate_is_valid, with the DataFrame API and with Spark SQL
print("=== Exercise 1 ===")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))
users_validated_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn(
        "birthdate_is_valid",
        F.coalesce(F.col("birthdate") <= F.to_date("first_ordered_at"), F.lit(True)),
    )
    .drop("first_ordered_at")
)

stg_users.createOrReplaceTempView("stg_users")
stg_orders.createOrReplaceTempView("stg_orders")
users_validated_sql = spark.sql("""
    SELECT u.*,
           coalesce(u.birthdate <= to_date(f.first_ordered_at), true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (
        SELECT user_id, min(ordered_at) AS first_ordered_at
        FROM stg_orders
        GROUP BY user_id
    ) f ON u.user_id = f.user_id
""").select(users_validated_df.columns)

print(
    "Rows differing between DataFrame and SQL:",
    users_validated_df.exceptAll(users_validated_sql).count(),
    users_validated_sql.exceptAll(users_validated_df).count(),
)
users_validated_df.groupBy("birthdate_is_valid").count().show()
users_validated_df.filter(~F.col("birthdate_is_valid")).select("username", "birthdate").show()
invalid_orders = stg_orders.join(stg_users, "user_id").filter(F.col("ordered_at") < F.col("birthdate")).count()
print("Orders placed before the birth of their user:", invalid_orders)

# Exercise 2: zip_code with regexp_extract, a Python UDF and a pandas_udf
print("=== Exercise 2 ===")
ZIP_PATTERN = "([A-Z]{2}) ([0-9]{5})$"
ZIP_RE = re.compile(ZIP_PATTERN)


@udf("string")
def zip_python_udf(address):
    match = ZIP_RE.search(address) if address is not None else None
    return match.group(2) if match else ""


@pandas_udf("string")
def zip_pandas_udf(addresses: pd.Series) -> pd.Series:
    return addresses.str.extract(ZIP_PATTERN, expand=True)[1].fillna("")


versions = {
    "regexp_extract": F.regexp_extract("address", ZIP_PATTERN, 2),
    "python udf": zip_python_udf("address"),
    "pandas_udf": zip_pandas_udf("address"),
}
datasets = {
    "users_bronze, 50 rows": users_bronze.select("address"),
    "users_bronze x 2000, 100,000 rows": users_bronze.select("address").crossJoin(spark.range(2000)).select("address"),
}
for dataset_name, dataset in datasets.items():
    dataset = dataset.cache()
    dataset.count()
    results = {}
    for name, expression in versions.items():
        def run(expression=expression):
            return dataset.select(expression.alias("zip_code")).agg(F.sum(F.length("zip_code"))).collect()
        run()  # warm-up
        seconds = min(timeit.repeat(run, number=1, repeat=5))
        results[name] = dataset.select(expression.alias("zip_code")).agg(F.sort_array(F.collect_list("zip_code"))).first()[0]
        print(f"{dataset_name} | {name}: best of 5 = {seconds:.3f} s")
    print(f"{dataset_name} | same zip codes:", results["regexp_extract"] == results["python udf"] == results["pandas_udf"])
    dataset.unpersist()

# Exercise 3: cache the bronze DataFrames
print("=== Exercise 3 ===")
timed("stg_orders.count, bronze read from S3", stg_orders.count)
timed("stg_users.count, bronze read from S3", stg_users.count)
users_bronze.cache()
orders_bronze.cache()
timed("materialize the cache", lambda: (users_bronze.count(), orders_bronze.count()))
timed("stg_orders.count, bronze cached", stg_orders.count)
timed("stg_users.count, bronze cached", stg_users.count)

# Exercise 4: append to the Iceberg table, time travel, and the same with Parquet
print("=== Exercise 4 ===")
NEW_ORDER_ID = "lab7-new-order-1"
new_order = (
    stg_orders.limit(1)
    .withColumn("order_id", F.lit(NEW_ORDER_ID))
    .withColumn("ordered_at", F.to_timestamp(F.lit("2020-04-28 12:00:00")))
)

orders_table = "lab.silver.orders"
if spark.table(orders_table).filter(F.col("order_id") == NEW_ORDER_ID).count() == 0:
    new_order.writeTo(orders_table).append()
spark.sql(f"""
    SELECT snapshot_id, parent_id, operation, committed_at
    FROM {orders_table}.snapshots
    ORDER BY committed_at
""").show(truncate=False)
first_snapshot = spark.sql(
    f"SELECT snapshot_id FROM {orders_table}.snapshots ORDER BY committed_at LIMIT 1"
).first()[0]
print("Iceberg, current version:", spark.table(orders_table).count(), "rows")
print(
    f"Iceberg, VERSION AS OF {first_snapshot}:",
    spark.sql(f"SELECT count(*) FROM {orders_table} VERSION AS OF {first_snapshot}").first()[0],
    "rows",
)

parquet_path = f"s3a://{BUCKET}/silver/spark/orders.parquet"
if spark.read.parquet(parquet_path).filter(F.col("order_id") == NEW_ORDER_ID).count() == 0:
    new_order.write.mode("append").parquet(parquet_path)
orders_parquet = spark.read.parquet(parquet_path)
print("Parquet directory:", orders_parquet.count(), "rows in", len(orders_parquet.inputFiles()), "files")
for path in orders_parquet.inputFiles():
    print(" ", path)