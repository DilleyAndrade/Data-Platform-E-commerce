from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType, LongType, DoubleType

ingestion_log_schema = StructType(
  [
    StructField("run_id", StringType(), False),
    StructField("source_name", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("file_name", StringType(), True),
    StructField("target_path", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("execution_status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("execution_date", DateType(), True)
  ]
)

landing_quality_log_schema = StructType(
  [
    StructField("run_id", StringType(), False),
    StructField("file_name", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("file_path", StringType(), True),
    StructField("check_name", StringType(), True),
    StructField("check_type", StringType(), True),
    StructField("records_total", LongType(), True),
    StructField("records_valid", LongType(), True),
    StructField("records_invalid", LongType(), True),
    StructField("invalid_percentage", DoubleType(), True),
    StructField("check_status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("execution_ts", TimestampType(), True)
  ]
)
