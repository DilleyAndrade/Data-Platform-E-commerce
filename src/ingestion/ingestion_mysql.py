import os
import io
import gc
import boto3
import pandas as pd
from datetime import date
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

tables = ["inventory", "order_items", "orders"]

def ingestion_mysql():

  engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")

  def get_s3_client():
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=endpoint if endpoint else None
    )
  
  s3 = get_s3_client()

  def create_dataframe(query):
    df = pd.read_sql(query, engine)

    print(df.head())
    return df


  def save_mysql_file(data_frame, table_name):
    try:
      bucket_name = "landing"
      key = f"{table_name}/ingestion_date_{date.today().strftime('%Y%m%d')}/{table_name}.parquet"

      with io.BytesIO() as buffer:
        data_frame.to_parquet(buffer, index=False)
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=buffer.getvalue()
        )
      del data_frame
      gc.collect()

      print(f"File {table_name} saved!")

    except Exception as e:
        print(f"Error to save file. Error: {e}")


  for table in tables:
    query = f"select * from {table};"
    save_mysql_file(create_dataframe(query), table)