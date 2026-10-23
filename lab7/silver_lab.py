"""Lab 7: bronze to silver with Spark DataFrames.

Code of the lab 07.sparksql-and-dataframes/lab-pyspark-silver.md.
In the PySpark shell, from the root of the project:
    pyspark --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0
    >>> exec(open("lab7/silver_lab.py").read())
"""
import configparser
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
ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "s3.seaweedfs.adm.adaltas.cloud")
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

# Fresh credentials written by the Onyxia script, they can be renewed without restarting the service
creds = configparser.ConfigParser()
creds.read(os.path.expanduser("~/.aws/credentials"))
aws = creds["default"]

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", aws["aws_access_key_id"])
    .config("spark.hadoop.fs.s3a.secret.key", aws["aws_secret_access_key"])
    .config("spark.hadoop.fs.s3a.session.token", aws["aws_session_token"])
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

# Bronze layer
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)

# Silver layer: users
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)

# regexp_extract() returns an empty string, not NULL, when there is no match
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))

print("Bronze:", users_bronze.count(), "users,", orders_bronze.count(), "orders")
stg_users.show(5, truncate=False)