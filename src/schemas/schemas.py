from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, LongType

ingestion_log_schema = StructType(
  [
    StructField("execution_id", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("target_path", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("duration_seconds", LongType(), True),
    StructField("execution_status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("execution_date", DateType(), True)
  ]
)