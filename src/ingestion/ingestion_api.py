from datetime import date, datetime
from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from observability.obs_ingestion_log import create_ingestion_log, write_ingestion_log
from path_constants.path_constants import (
    BUCKET_LAN,
    URL_CUSTOMER_REVIEW,
    URL_EXCHANGE_RATES,
    URL_MARKETING_CAMPAIGNS,
)
from utils.logger import log
from utils.s3_transfer import S3_TRANSFER_CONFIG

CONNECT_TIMEOUT_SECONDS = 10
READ_TIMEOUT_SECONDS = 120
HTTP_RETRY_ATTEMPTS = 3

API_SOURCES = (
    (URL_CUSTOMER_REVIEW, "customer_review"),
    (URL_EXCHANGE_RATES, "exchange_rates"),
    (URL_MARKETING_CAMPAIGNS, "marketing_campaigns"),
)

# prepara um cliente HTTP reutilizável para consultar as APIs com tratamento automático de falhas temporárias.
# Uma Session reaproveita conexões HTTP. Em vez de abrir uma nova conexão para cada endpoint, ela pode reutilizar a conexão existente, reduzindo tempo e custo de rede.
def create_http_session() -> requests.Session:
    retry = Retry(
        total=HTTP_RETRY_ATTEMPTS,
        connect=HTTP_RETRY_ATTEMPTS,
        read=HTTP_RETRY_ATTEMPTS,
        status=HTTP_RETRY_ATTEMPTS,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def build_landing_location(dataset: str, ingestion_date: date) -> tuple[str, str]:
    date_partition = ingestion_date.strftime("%Y%m%d")
    directory = f"{dataset}/ingestion_date_{date_partition}"
    key = f"{directory}/{dataset}.json"
    s3_path = f"s3://{BUCKET_LAN}/{directory}/"
    return key, s3_path

#Transfere os dados de uma API diretamente para o S3/MinIO, sem carregar a resposta inteira na memória.
def stream_api_to_s3(
    http_session: requests.Session,
    s3_client: Any,
    url: str,
    dataset: str,
    ingestion_date: date,
) -> str:
    key, s3_path = build_landing_location(dataset, ingestion_date)
    log.info("Streaming API %s to %s.", url, key)

    with http_session.get(
        url,
        stream=True,
        timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS),
        headers={"Accept": "application/json"},
    ) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        s3_client.upload_fileobj(
            response.raw,
            BUCKET_LAN,
            key,
            ExtraArgs={"ContentType": "application/json"},
            Config=S3_TRANSFER_CONFIG,
        )

    log.info("Successfully streamed %s to S3.", dataset)
    return s3_path


def ingestion_api(
    spark: Any,
    run_id: str,
    ingestion_date: date,
    s3_client: Any,
    http_session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    log.info("Started API ingestion.")
    if s3_client is None:
        raise ConnectionError("Could not create the S3/MinIO client.")

    owns_session = http_session is None
    session = http_session if http_session is not None else create_http_session()
    ingestion_logs: list[dict[str, Any]] = []

    try:
        for url, dataset in API_SOURCES:
            started_at = datetime.now()
            _, s3_path = build_landing_location(dataset, ingestion_date)

            try:
                s3_path = stream_api_to_s3(
                    session,
                    s3_client,
                    url,
                    dataset,
                    ingestion_date,
                )
                status = "SUCCESS"
                error_message = ""
            except Exception as error:
                status = "FAILED"
                error_message = f"{type(error).__name__}: {error}"
                log.exception("Failed to ingest API dataset %s from %s.", dataset, url)

            ended_at = datetime.now()
            ingestion_logs.append(
                create_ingestion_log(
                    run_id,
                    "api",
                    dataset,
                    "api",
                    f"{dataset}.json",
                    s3_path,
                    started_at,
                    ended_at,
                    status,
                    error_message,
                )
            )
    finally:
        if owns_session:
            session.close()

    write_ingestion_log(spark, ingestion_logs)
    log.info("Finished API ingestion.")
    return ingestion_logs
