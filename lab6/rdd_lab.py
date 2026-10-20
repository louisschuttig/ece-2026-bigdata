"""Lab 6: introduction to Spark's RDD API.

Code of the lab 06.spark-introduction/lab-pyspark-rdd.md, with the exercises.
Run it from the root of the project, after downloading the bronze datasets:

    aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv
    aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv
    spark-submit lab6/rdd_lab.py 2>/dev/null
"""
import re
import time
from operator import add

from pyspark.sql import SparkSession

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


def timed(label, action):
    start = time.perf_counter()
    result = action()
    print(f"{label}: {result} in {time.perf_counter() - start:.3f} s")


def main():
    spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # RDD creation and partitions
    users_lines = sc.textFile("users.csv")
    orders_lines = sc.textFile("orders.csv")
    print("Partitions:", users_lines.getNumPartitions(), orders_lines.getNumPartitions())
    print("After repartition(8):", orders_lines.repartition(8).getNumPartitions())

    # Lazy evaluation and lineage
    header = orders_lines.first()
    orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
    print(orders_data.toDebugString().decode())
    print("Orders:", orders_data.count())

    # The cost of no schema: the multiline address
    print("Physical lines in users.csv:", users_lines.count())
    users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
    print("User records:", users_records.count())

    # Manual parsing of orders
    orders_rdd = orders_data.map(parse_order).cache()
    print(orders_rdd.take(1))

    # Key-value RDDs: total quantity per user, reduceByKey vs groupByKey
    quantities = orders_rdd.map(lambda o: (o["user_id"], o["quantity"]))
    user_quantity = quantities.reduceByKey(add)
    user_quantity_grouped = quantities.groupByKey().mapValues(sum)
    print("Same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

    # Manual deduplication
    orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
    print("Before/after deduplication:", orders_rdd.count(), orders_dedup.count())

    # Joining orders with users
    users_kv = users_records.map(parse_user_for_join)
    orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
    enriched = orders_kv.join(users_kv)
    print(enriched.take(1))
    print("join / leftOuterJoin:", enriched.count(), orders_kv.leftOuterJoin(users_kv).count())

    # Exercise 1: (order_count, total_quantity) per user with aggregateByKey
    stats = quantities.aggregateByKey(
        (0, 0),
        lambda acc, quantity: (acc[0] + 1, acc[1] + quantity),
        lambda a, b: (a[0] + b[0], a[1] + b[1]),
    )
    avg_quantity = stats.mapValues(lambda s: round(s[1] / s[0], 2))
    print(stats.take(3))
    print(avg_quantity.take(3))

    # Exercise 2: an order with an unknown user_id, found with leftOuterJoin
    fake = sc.parallelize([{
        "order_id": "fake-order-1",
        "user_id": "00000000-0000-0000-0000-000000000000",
        "date": "2020-05-01 00:00:00+00:00",
        "quantity": 1,
        "product": "bread",
    }])
    orphans = (
        orders_rdd.union(fake)
        .map(lambda o: (o["user_id"], o))
        .leftOuterJoin(users_kv)
        .filter(lambda kv: kv[1][1] is None)
    )
    print("Orphan orders:", orphans.count(), orphans.collect())

    # Exercise 3: the same aggregation with the DataFrame API (the CSV column is user_uuid)
    orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
    user_quantity_df = orders_df.groupBy("user_uuid").sum("quantity")
    user_quantity_df.show(5)
    user_quantity_df.explain()
    print(user_quantity.toDebugString().decode())

    # Exercise 4: with and without cache
    timed("cached orders_rdd.count", orders_rdd.count)
    timed("cached user_quantity.count", user_quantity.count)
    orders_rdd.unpersist()
    orders_nocache = orders_data.map(parse_order)
    user_quantity_nocache = orders_nocache.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
    timed("no cache orders.count", orders_nocache.count)
    timed("no cache user_quantity.count", user_quantity_nocache.count)

    spark.stop()


if __name__ == "__main__":
    main()
