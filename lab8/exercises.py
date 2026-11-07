"""Lab 8: exercises on lab.silver.users.

Run after lab8/session.py, in the same PySpark shell:
    >>> exec(open("lab8/exercises.py").read())
Each write is skipped if it has already been done.
"""
from pyspark.sql import functions as F

USERS = "lab.silver.users"


def show_snapshots(table):
    spark.sql(f"""
        SELECT snapshot_id, parent_id, operation, committed_at
        FROM {table}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)


# Exercise 1: lab.silver.users is already an Iceberg table, written by lab7/write.py
print("=== Exercise 1 ===")
users = spark.table(USERS)
print("Rows:", users.count())
users.printSchema()
show_snapshots(USERS)

# Exercise 2: append a new batch of users, with new identifiers, usernames and emails
print("=== Exercise 2 ===")
if spark.table(USERS).filter(F.col("username").endswith("_batch2")).count() == 0:
    new_users = (
        users.limit(3)
        .withColumn("user_id", F.expr("uuid()"))
        .withColumn("username", F.concat("username", F.lit("_batch2")))
        .withColumn("email", F.concat(F.lit("batch2."), "email"))
    )
    new_users.writeTo(USERS).append()
show_snapshots(USERS)
print("Rows:", spark.table(USERS).count())

# Exercise 3: the table before and after the append, with time travel
print("=== Exercise 3 ===")
for snapshot in spark.sql(f"SELECT snapshot_id, operation FROM {USERS}.snapshots ORDER BY committed_at").collect():
    rows = spark.sql(f"SELECT count(*) FROM {USERS} VERSION AS OF {snapshot['snapshot_id']}").first()[0]
    print(f"VERSION AS OF {snapshot['snapshot_id']} ({snapshot['operation']}): {rows} rows")

# Exercise 4: add a country column and write a row with the evolved schema
print("=== Exercise 4 ===")
if "country" not in spark.table(USERS).columns:
    spark.sql(f"ALTER TABLE {USERS} ADD COLUMN country STRING")
    spark.sql(f"""
        INSERT INTO {USERS}
        SELECT uuid(), concat(username, '_us'), name, sex, concat('us.', email), birthdate,
               street, city, state, zip_code, 'US'
        FROM {USERS}
        LIMIT 1
    """)
spark.sql(f"DESCRIBE {USERS}").show()
spark.sql(f"SELECT country, count(*) AS users FROM {USERS} GROUP BY country").show()
show_snapshots(USERS)
spark.sql(f"""
    SELECT file_path, record_count, file_size_in_bytes
    FROM {USERS}.files
""").show(truncate=False)
spark.sql(f"""
    SELECT timestamp, file, latest_snapshot_id
    FROM {USERS}.metadata_log_entries
    ORDER BY timestamp
""").show(truncate=False)