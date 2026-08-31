import os
import boto3
from utils.logger import log
from dotenv import load_dotenv
from botocore.exceptions import EndpointConnectionError, ClientError

load_dotenv()


def get_s3_client():
    endpoint = os.getenv("AWS_ENDPOINT_URL")

    client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=endpoint if endpoint else None,
    )

    try:
        client.list_buckets()
        log.info("Connection to S3/MinIO successfully established!")
        return client
    except EndpointConnectionError:
        log.error(f"Service offline or inaccessible at the endpoint: {endpoint}")

    except ClientError as e:
        log.error(f"Credential or permission error on S3/MinIO: {e}")
