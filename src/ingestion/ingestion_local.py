import os
import boto3
from datetime import date
from dotenv import load_dotenv


load_dotenv()

BUCKET_NAME = "landing"
SOURCE_DIRECTORY = "C:/Users/dille/OneDrive/Desktop/data-platform/local_data_source"

def ingestion_local():

  def get_s3_client():
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=endpoint if endpoint else None,
    )


  s3 = get_s3_client()

  files = os.listdir(SOURCE_DIRECTORY)

  for file in files:
    file_name_splited = file.split(".")
    table_name = file_name_splited[0]
    table_path = f"{SOURCE_DIRECTORY}/{file}"

    key = f"{table_name}/ingestion_date_{date.today().strftime('%Y%m%d')}/{file}"

    print(table_name)
    print(table_path)

    s3.upload_file(
      Filename=table_path,
      Bucket=BUCKET_NAME,
      Key=key
    )
    print(files)