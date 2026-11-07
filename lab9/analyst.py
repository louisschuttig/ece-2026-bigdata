"""Lab 9: as analyst, read through Polaris, then try to write.

Run after lab9/session.py started with POLARIS_PRINCIPAL=analyst:
    >>> exec(open("lab9/analyst.py").read())
"""
assert POLARIS_PRINCIPAL == "analyst", "restart the session with POLARIS_PRINCIPAL=analyst"


def try_sql(label, statement):
    try:
        spark.sql(statement)
        print(f"{label}: SUCCEEDED")
    except Exception as error:
        print(f"{label}: FAILED")
        for line in [line for line in str(error).splitlines() if line.strip()][:3]:
            print("   ", line[:300])


print("=== Read as analyst ===")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("""
    SELECT product, count(*) AS orders
    FROM polaris.silver.orders
    GROUP BY product
    ORDER BY product
""").show()

print("=== Write as analyst ===")
try_sql("INSERT INTO polaris.silver.orders", "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
try_sql("ALTER TABLE polaris.silver.users ADD COLUMN phone", "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM polaris.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)

# What Polaris cannot enforce here: the session holds the S3 keys of the bucket owner
print("=== Bypass Polaris with the S3 keys of the session ===")
print("orders read through the Hadoop catalog:", spark.table("lab.silver.orders").count())
print("orders data files read directly:", spark.read.parquet(f"s3a://{BUCKET}/warehouse/silver/orders/data/").count())