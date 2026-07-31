import io
import os
import gc
import boto3
import requests
import pandas as pd
from dotenv import load_dotenv
from path_constants.path_constants import URL_CUSTOMER_REVIEW, URL_EXCHANGE_RATES, URL_MARKETING_CAMPAIGNS
from datetime import date

load_dotenv()

urls = [
    [URL_CUSTOMER_REVIEW, "customer_review"], 
    [URL_EXCHANGE_RATES, "exchange_rates"], 
    [URL_MARKETING_CAMPAIGNS, "marketing_campaigns"]
]

def ingestion_api():
    #S3 connection
    def get_s3_client():
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        return boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=endpoint if endpoint else None
        )


    s3 = get_s3_client()

    #Save file in s3
    def save_api_file(api_url, file_name):
        try:
            bucket_name = "landing"
            key = f"{file_name}/ingestion_date_{date.today().strftime('%Y%m%d')}/{file_name}.parquet"

           #Alguns dados da api chegavam como dict e outros nao, isso faz um tratamento para padronizar os arquivos e todos serem dict []
            if isinstance(api_url, dict):
                df = pd.DataFrame([api_url])
            else:
                df = pd.DataFrame(api_url)

            #O io salva recursos na memoria, usando assim o recurso eh liberado quando salvar o arquivo no minio
            with io.BytesIO() as buffer:
                df.to_parquet(buffer, index=False)
                s3.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=buffer.getvalue()
                )
            del df #Apaga as referencias dos dados passados na ram
            gc.collect() # Forca o Garbage collector a recolher os residuops da memoria

            print(f"File {file_name} saved!")

        except Exception as e:
            print(f"Error to save file. Error: {e}")

    #Read api
    def read_api(url):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            print(f"Connected with: {url}")
            print(data)
            return data

        except requests.exceptions.HTTPError as errh:
            print(f"HTTP error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Conection error: {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"Timeout exceeded: {errt}")
        except requests.exceptions.RequestException as err:
            print(f"Unexpected error: {err}")


    for url, file_name in urls:
        save_api_file(read_api(url), file_name)