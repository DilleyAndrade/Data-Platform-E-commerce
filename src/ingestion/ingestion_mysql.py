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

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

tables = ["inventory", "order_items", "orders"]

def ingestion_mysql(spark):

  log.info("Started mySql Ingestion.")
  engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")

  s3 = get_s3_client()

  def create_dataframe(query):
    df = pd.read_sql(query, engine)
    return df

  def save_mysql_file(data_frame, table_name):
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

    query = f"select * from {table};"
    
    try:
      save_mysql_file(create_dataframe(query), table)
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
    create_ingestion_log_table(spark, "mysql", table, "table", table, s3_path, started_at, ended_at, status, error_message)
    log.info(f"Ingestion log table created for {table}.")

  log.info("Finished mySql Ingestion.")