"""Lab 9: as dev, register lab.silver.orders in Polaris, create polaris.silver.users and change its schema.

Run after lab9/session.py started with POLARIS_PRINCIPAL=dev:
    >>> exec(open("lab9/dev.py").read())
Each write is skipped if it has already been done.
"""
assert POLARIS_PRINCIPAL == "dev", "restart the session with POLARIS_PRINCIPAL=dev"


def polaris_tables():
    return [row.tableName for row in spark.sql("SHOW TABLES IN polaris.silver").collect()]


# Register the Silver table: Polaris points to the current metadata file, nothing is copied
print("=== Register lab.silver.orders ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
metadata_dir = f"s3a://{BUCKET}/warehouse/silver/orders/metadata"
version = spark.read.text(f"{metadata_dir}/version-hint.text").first()[0].strip()
metadata_file = f"{metadata_dir}/v{version}.metadata.json"
print("Current metadata file of lab.silver.orders:", metadata_file)
if "orders" not in polaris_tables():
    spark.sql(f"""
        CALL polaris.system.register_table(
          table => 'silver.orders',
          metadata_file => '{metadata_file}'
        )
    """).show(truncate=False)
spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
spark.sql("SELECT count(*) AS orders FROM lab.silver.orders").show()

# Create tables as dev
print("=== Tables in polaris.silver ===")
spark.sql("SHOW TABLES IN polaris.silver").show()
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM polaris.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)

print("=== Create polaris.silver.users ===")
if "users" not in polaris_tables():
    # lab.silver.users already has the country column of the Iceberg lab: drop it, the next section adds it
    stg_users = spark.table("lab.silver.users").drop("country")
    stg_users.writeTo("polaris.silver.users").createOrReplace()
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
spark.sql("SELECT file_path, record_count FROM polaris.silver.users.files").show(truncate=False)

# Schema change as dev
print("=== Schema change ===")
if "country" not in spark.table("polaris.silver.users").columns:
    spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
spark.sql("DESCRIBE polaris.silver.users").show()
spark.sql("""
    SELECT timestamp, file, latest_snapshot_id
    FROM polaris.silver.users.metadata_log_entries
    ORDER BY timestamp
""").show(truncate=False)