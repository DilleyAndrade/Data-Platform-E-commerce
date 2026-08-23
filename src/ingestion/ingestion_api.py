import io
import gc
import requests
from observability.obs_ingestion_log import create_ingestion_log_table
from utils.logger import log
import pandas as pd
from dotenv import load_dotenv
from path_constants.path_constants import URL_CUSTOMER_REVIEW, URL_EXCHANGE_RATES, URL_MARKETING_CAMPAIGNS
from datetime import date, datetime
from path_constants.path_constants import BUCKET_LAN

from utils.s3_client import get_s3_client

load_dotenv()

urls = [
    [URL_CUSTOMER_REVIEW, "customer_review"], 
    [URL_EXCHANGE_RATES, "exchange_rates"], 
    [URL_MARKETING_CAMPAIGNS, "marketing_campaigns"]
]

def ingestion_api(spark):

    log.info("Started Api Ingestion.")
    s3 = get_s3_client()

    def save_api_file(api_url, file_name):
        try:
            log.info(f"Trying save {file_name} in S3.")
            
            ingestion_date = date.today().strftime('%Y%m%d')
            key = f"{file_name}/ingestion_date_{ingestion_date}/{file_name}.parquet"

           #Alguns dados da api chegavam como dict e outros nao, isso faz um tratamento para padronizar os arquivos e todos serem dict []
            if isinstance(api_url, dict):
                df = pd.DataFrame([api_url])
            else:
                df = pd.DataFrame(api_url)

            #O io salva recursos na memoria, usando assim o recurso eh liberado quando salvar o arquivo no minio
            with io.BytesIO() as buffer:
                df.to_parquet(buffer, index=False)
                s3.put_object(
                    Bucket=BUCKET_LAN,
                    Key=key,
                    Body=buffer.getvalue()
                )
            del df #Apaga as referencias dos dados passados na ram
            gc.collect() # Forca o Garbage collector a recolher os residuops da memoria

            log.info(f"Success to save {file_name} in S3.")

        except Exception as e:
            log.info(f"Failed to save {file_name} in S3.")
            log.error(f"ERROR: {e}")

    #Read api
    def read_api(url):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            log.info(f"Connected with: {url}")
            return data

        except requests.exceptions.HTTPError as errh:
            log.error(f"HTTP error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            log.error(f"Conection error: {errc}")
        except requests.exceptions.Timeout as errt:
            log.error(f"Timeout exceeded: {errt}")
        except requests.exceptions.RequestException as err:
            log.error(f"Unexpected error: {err}")


    for url, file_name in urls:
        started_at = datetime.now()
        log.info(f"Reading file {file_name}.")

        try:
            save_api_file(read_api(url), file_name)
            status = "SUCCESS"
            error_message = ""

        except Exception as e:
            status = "FAILED"
            error_message = f"Error: {e}"
            log.info(f"Failed to save {file_name} in S3.")
            log.error(f"ERROR: {e}")

        ended_at =  datetime.now()

        ingestion_date = date.today().strftime('%Y%m%d')
        s3_path = f"s3://{BUCKET_LAN}/{file_name}/ingestion_date_{ingestion_date}/"
    
        log.info(f"Creating ingestion log table for file {file_name}.")
        create_ingestion_log_table(spark, "api", file_name, "api", file_name, s3_path, started_at, ended_at, status, error_message)
        log.info(f"Ingestion log table created for {file_name}.")

    log.info("Finished Api Ingestion.")