from uuid import uuid4
from datetime import date
from schemas.schemas import ingestion_log_schema
from path_constants.path_constants import BUCKET_OBS

uuid_num = uuid4()

def create_ingestion_log_table(
  spark,
  source_name,
  source_table,
  source_type,
  file_name,
  target_path,
  started_at,
  ended_at,
  execution_status,
  error_message
):
  run_id = f"{source_name}_ingestion_{uuid_num}"

  ingestion_log_columns = [
    {
      "execution_id": run_id,
      "source_name": source_name,
      "source_table": source_table,
      "source_type": source_type,
      "file_name": file_name,
      "target_path": target_path,
      "started_at": started_at,
      "ended_at": ended_at,
      "duration_seconds": int((ended_at-started_at).total_seconds()),
      "execution_status": execution_status,
      "error_message": error_message,
      "execution_date": date.today()
    }
  ]

  df_ingestion_log = spark.createDataFrame(
    ingestion_log_columns,
    schema=ingestion_log_schema
  )

  s3_path = f"s3a://{BUCKET_OBS}/ingestion_log"
  
  df_ingestion_log.coalesce(1).write \
    .format("delta") \
    .mode("append") \
    .save(s3_path)

  spark.sql(f"OPTIMIZE delta.`{s3_path}`")

  spark.sql(f"VACUUM delta.`{s3_path}` RETAIN 168 HOURS")
 
  return df_ingestion_log