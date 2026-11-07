"""Lab 9: Polaris, SparkSession with the Hadoop catalog `lab` and the Polaris catalog `polaris`.

From the root of the project, in a terminal:
    eval "$(aws configure export-credentials --profile default --format env)"
    export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE" POLARIS_URI="http://polaris:8181" POLARIS_CATALOG="$KUBERNETES_NAMESPACE" AWS_REGION="us-east-1"
    set -a; . /home/onyxia/work/.polaris-credentials.env; set +a
    POLARIS_PRINCIPAL=dev pyspark --master local[*] --driver-memory 1g \
      --conf spark.local.dir=/home/onyxia/work/.spark-tmp --conf spark.jars.ivy=/home/onyxia/work/.ivy2 \
      --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
      --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    >>> exec(open("lab9/session.py").read())
POLARIS_PRINCIPAL selects the Polaris principal: dev (default) or analyst.
"""
import os

from pyspark.sql import SparkSession

# The session created by the pyspark shell cannot take the S3A, Iceberg and Polaris settings
try:
    spark.stop()
except NameError:
    pass

POLARIS_PRINCIPAL = os.environ.get("POLARIS_PRINCIPAL", "dev")
POLARIS_CREDENTIAL = os.environ[f"POLARIS_{POLARIS_PRINCIPAL.upper()}_CREDENTIAL"]

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-polaris")
    .master("local[*]")
    # S3 access, identical to the Spark lab
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Hadoop catalog of the Spark lab
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    # Polaris catalog, REST API, principal chosen by POLARIS_PRINCIPAL
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    # No credential vending on this storage: files are read with the S3A keys above
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("Polaris principal:", POLARIS_PRINCIPAL)
print("fs.s3.impl:", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))
print("=== SHOW NAMESPACES IN lab ===")
spark.sql("SHOW NAMESPACES IN lab").show()
print("=== SHOW NAMESPACES IN polaris ===")
try:
    spark.sql("SHOW NAMESPACES IN polaris").show()
except Exception as error:
    print("FAILED:", next((line for line in str(error).splitlines() if "Forbidden" in line), str(error).splitlines()[0]).strip())