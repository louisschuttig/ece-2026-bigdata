"""Lab 8: second snapshot, time travel and schema evolution on lab.silver.orders.

Run after lab8/session.py, in the same PySpark shell:
    >>> exec(open("lab8/changes.py").read())
The writes are skipped if the file has already run (the column `source` exists).
"""
from pyspark.sql import functions as F

TABLE = "lab.silver.orders"
already_done = "source" in spark.table(TABLE).columns


def show_snapshots():
    spark.sql(f"""
        SELECT snapshot_id, parent_id, operation, committed_at
        FROM {TABLE}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)


# Create a second snapshot: read the orders from the Iceberg table, append 3 new ones
print("=== Append 3 orders ===")
stg_orders = spark.table(TABLE)
stg_orders.show(5)
if not already_done:
    new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
    new_orders.writeTo(TABLE).append()
show_snapshots()
spark.sql(f"SELECT count(*) FROM {TABLE}").show()

# Time travel: count the rows of every snapshot
print("=== Time travel ===")
snapshots = spark.sql(f"""
    SELECT snapshot_id, operation
    FROM {TABLE}.snapshots
    ORDER BY committed_at
""").collect()
print(snapshots)
for snapshot in snapshots:
    rows = spark.sql(f"SELECT count(*) FROM {TABLE} VERSION AS OF {snapshot['snapshot_id']}").first()[0]
    print(f"VERSION AS OF {snapshot['snapshot_id']} ({snapshot['operation']}): {rows} rows")
spark.sql(f"SELECT count(*) FROM {TABLE}").show()

# Schema evolution
print("=== Schema evolution ===")
if not already_done:
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN source STRING")
spark.sql(f"DESCRIBE {TABLE}").show()
if not already_done:
    spark.sql(f"""
        INSERT INTO {TABLE}
        SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
        FROM {TABLE}
        LIMIT 1
    """)
spark.sql(f"SELECT source, count(*) AS orders FROM {TABLE} GROUP BY source").show()

# State of the table after the changes
print("=== Snapshots, files and metadata versions ===")
show_snapshots()
spark.sql(f"SELECT file_path, record_count, file_size_in_bytes FROM {TABLE}.files").show(truncate=False)
spark.sql(f"SELECT timestamp, file, latest_snapshot_id FROM {TABLE}.metadata_log_entries ORDER BY timestamp").show(truncate=False)