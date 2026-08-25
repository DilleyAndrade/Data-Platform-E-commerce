import os
from pyspark.sql import SparkSession

DEFAULT_SPARK_WAREHOUSE = "s3a://observability/spark-warehouse/"
SPARK_PACKAGES = ",".join(
    (
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.mysql:mysql-connector-j:8.0.33",
        "org.postgresql:postgresql:42.7.3",
    )
)


def spark_session(app_name: str, master_mode: str):
    warehouse_directory = os.getenv(
        "SPARK_WAREHOUSE_DIR",
        DEFAULT_SPARK_WAREHOUSE,
    )

    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_mode)
        .config(
            "spark.jars.packages",
            SPARK_PACKAGES,
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_directory)
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_ENDPOINT_URL"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
