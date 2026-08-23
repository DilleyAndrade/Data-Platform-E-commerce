import os
import io
import gc
import pandas as pd
from datetime import date, datetime
from observability.obs_ingestion_log import create_ingestion_log_table
from utils.logger import log
from dotenv import load_dotenv
from sqlalchemy import create_engine
from path_constants.path_constants import BUCKET_LAN
from utils.s3_client import get_s3_client

load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

tables = ["customers", "products", "suppliers"]

def ingestion_postgres(spark):

  log.info("Started postgres Ingestion.")
  engine = create_engine(f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")
  
  s3 = get_s3_client()

  def create_dataframe(query):
    df = pd.read_sql(query, engine)
    return df

  def save_postgres_file(data_frame, table_name):
    try:
      log.info(f"Trying save {table_name} in S3.")
      s3_key = f"{table_name}/ingestion_date_{date.today().strftime('%Y%m%d')}/{table_name}.parquet"

      with io.BytesIO() as buffer:
        data_frame.to_parquet(buffer, index=False)
        s3.put_object(
            Bucket=BUCKET_LAN,
            Key=s3_key,
            Body=buffer.getvalue()
        )
      del data_frame
      gc.collect()

      log.info(f"Success to save {table_name} in S3.")

    except Exception as e:
        log.info(f"Failed to save {table_name} in S3.")
        log.error(f"ERROR: {e}")

  for table in tables:
    started_at = datetime.now()
    log.info(f"Reading file {table}.")

    query = f"select * from public.{table};"

    try:
      save_postgres_file(create_dataframe(query), table)
      status = "SUCCESS"
      error_message = ""
    except Exception as e:
      status = "FAILED"
      error_message = f"Error: {e}"
      log.info(f"Failed to save {table} in S3.")
      log.error(f"ERROR: {e}")

    ended_at =  datetime.now()

    ingestion_date = date.today().strftime('%Y%m%d')
    s3_path = f"s3://{BUCKET_LAN}/{table}/ingestion_date_{ingestion_date}/"

    log.info(f"Creating ingestion log table for file {table}.")
    create_ingestion_log_table(spark, "postgres", table, "table", table, s3_path, started_at, ended_at, status, error_message)
    log.info(f"Ingestion log table created for {table}.")

  log.info("Finished postgres Ingestion.")