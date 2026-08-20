import os
from utils.logger import log
from dotenv import load_dotenv
from datetime import date, datetime
from utils.s3_client import get_s3_client
from observability.obs_ingestion_log import create_ingestion_log_table
from path_constants.path_constants import PATH_LOCAL_FILES, BUCKET_LAN

load_dotenv()

def ingestion_local(spark):

  log.info("Started local Ingestion.")
  
  s3 = get_s3_client()

  files = os.listdir(PATH_LOCAL_FILES)

  log.info(f"Searching files in {PATH_LOCAL_FILES}.")

  if not files:
    log.info(f"No files in {PATH_LOCAL_FILES}.")
  else:

    for file in files:
      started_at = datetime.now()

      log.info(f"Reading file {file}.")

      status = ""
      error_message = ""
      file_name_splited = file.split(".")
      file_name = file_name_splited[0]
      file_format = file_name_splited[1]
      table_path = f"{PATH_LOCAL_FILES}/{file}"
      ingestion_date = date.today().strftime('%Y%m%d')

      s3_path = f"s3://{BUCKET_LAN}/{file_name}/ingestion_date_{ingestion_date}/"
      s3_key = f"{file_name}/ingestion_date_{ingestion_date}/{file}"
      

      try:
        log.info(f"Trying save {file_name} in S3.")
        s3.upload_file(
          Filename=table_path,
          Bucket=BUCKET_LAN,
          Key=s3_key
        )
        log.info(f"Success to save {file_name} in S3.")
        status = "SUCCESS"
        error_message = ""
        
      except Exception as e:
        status = "FAILED"
        error_message = f"Error: {e}"
        log.info(f"Failed to save {file_name} in S3.")
        log.error(f"ERROR: {e}")

      ended_at = datetime.now()
    
      log.info(f"Creating ingestion log table for file {file_name}.")
      create_ingestion_log_table(spark, "local", file_name, file_format, file, s3_path, started_at, ended_at, status, error_message)
      log.info(f"Ingestion log table created for {file_name}.")
  
  log.info("Finished local Ingestion.")
