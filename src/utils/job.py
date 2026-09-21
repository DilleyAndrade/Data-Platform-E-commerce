import argparse
import os
from contextlib import contextmanager
from datetime import date

from utils.s3_client import get_s3_client
from utils.spark_session import spark_session


def job_arguments(description: str):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--date",
        dest="execution_date",
        type=date.fromisoformat,
        required=True,
    )
    return parser.parse_args()


@contextmanager
def job_spark(app_name: str):
    spark = spark_session(app_name, os.getenv("SPARK_MASTER", "local[*]"))
    try:
        yield spark
    finally:
        spark.stop()


def required_s3_client():
    client = get_s3_client()
    if client is None:
        raise ConnectionError("Could not create the S3/MinIO client.")
    return client


def raise_for_failed_events(events, stage):
    failures = [
        event
        for event in events
        if str(event.get("status", event.get("execution_status", ""))).upper()
        == "FAILED"
    ]
    if failures:
        names = [
            event.get("dataset_name")
            or event.get("source_table")
            or event.get("target_table")
            or event.get("file_name")
            or "unknown"
            for event in failures
        ]
        raise RuntimeError(f"{stage} failed for: {', '.join(map(str, names))}")
