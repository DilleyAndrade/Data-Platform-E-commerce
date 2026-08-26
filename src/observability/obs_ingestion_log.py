from datetime import date
from path_constants.path_constants import BUCKET_OBS
from schemas.schemas import ingestion_log_schema


def create_ingestion_log(run_id, source_name, source_table, source_type, file_name,
                         target_path, started_at, ended_at, status, error_message):
    return {
        "run_id": run_id,
        "source_name": source_name,
        "source_table": source_table,
        "source_type": source_type,
        "file_name": file_name,
        "target_path": target_path,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": (ended_at - started_at).total_seconds(),
        "execution_status": status,
        "error_message": error_message,
        "execution_date": date.today(),
    }


def write_ingestion_log(spark, logs):
    if not logs:
        return None
    dataframe = spark.createDataFrame(logs, schema=ingestion_log_schema)
    dataframe.write.format("delta").mode("append").save(f"s3a://{BUCKET_OBS}/ingestion_log")
    return dataframe
