"""Lab 8: Iceberg tables with Spark, session and inspection of the table.

From the root of the project, in a terminal:
    eval "$(aws configure export-credentials --profile default --format env)"
    pyspark --master local[*] --driver-memory 1g --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
    >>> exec(open("lab8/session.py").read())
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# The session created by the pyspark shell cannot take the S3A and Iceberg settings
try:
    spark.stop()
except NameError:
    pass

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .master("local[*]")
    # S3 access, with the credentials exported from the default profile
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("=== Catalog ===")
spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW TABLES IN lab.silver").show()

# The table already exists: it was written by the Spark DataFrame lab
print("=== Query the table through the catalog ===")
spark.sql("""
    SELECT count(*) AS orders,
           sum(quantity) AS quantity
    FROM lab.silver.orders
""").show()

print("=== Snapshots ===")
spark.sql("SELECT * FROM lab.silver.orders.snapshots").show(truncate=False, vertical=True)

print("=== Files ===")
spark.sql("""
    SELECT file_path,
           file_format,
           record_count,
           file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)