from datetime import date
from uuid import uuid4
from data_quality.dq_landing_raw import dq_landing_raw
from data_correction.data_correction import data_correction
from transformation.transformation_raw_bronze import transformation_raw_bronze
from ingestion.ingestion_api import ingestion_api
from ingestion.ingestion_local import ingestion_local
from ingestion.ingestion_mysql import ingestion_mysql
from ingestion.ingestion_postgres import ingestion_postgres
from utils.s3_client import get_s3_client
from utils.spark_session import spark_session


def run_pipeline(run_id: str, ingestion_date: date) -> None:
    
    spark = spark_session("ingestion_pipeline", "local[*]")
    s3_client = get_s3_client()

    if s3_client is None:
        spark.stop()
        raise ConnectionError("Could not create the S3/MinIO client.")

    try:
        ingestion_local(spark, run_id, ingestion_date, s3_client)
        ingestion_postgres(spark, run_id, ingestion_date)
        ingestion_mysql(spark, run_id, ingestion_date)
        ingestion_api(spark, run_id, ingestion_date, s3_client)
        dq_landing_raw(spark, run_id, ingestion_date, s3_client)
        data_correction(spark, f"correction_{run_id}", ingestion_date, s3_client)
        transformation_raw_bronze(spark, run_id, ingestion_date, s3_client)
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline(
        run_id=f"ingestion_{uuid4()}",
        ingestion_date=date.today(),
    )
