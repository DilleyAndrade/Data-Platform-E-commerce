from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from utils import job, s3_client, s3_transfer, spark_session


def test_raise_for_failed_events_accepts_both_status_contracts():
    with pytest.raises(RuntimeError, match="orders"):
        job.raise_for_failed_events([{"status": "FAILED", "dataset_name": "orders"}], "Silver")
    with pytest.raises(RuntimeError, match="reviews"):
        job.raise_for_failed_events([{"execution_status": "failed", "source_table": "reviews"}], "Ingestion")
    job.raise_for_failed_events([{"status": "SUCCESS"}], "Stage")


def test_required_s3_client_converts_none_to_connection_error(monkeypatch):
    monkeypatch.setattr(job, "get_s3_client", lambda: None)
    with pytest.raises(ConnectionError, match="S3/MinIO"):
        job.required_s3_client()


def test_job_spark_stops_session_even_when_job_fails(monkeypatch):
    session = MagicMock()
    monkeypatch.setattr(job, "spark_session", lambda name, master: session)
    monkeypatch.setenv("SPARK_MASTER", "local[1]")
    with pytest.raises(RuntimeError):
        with job.job_spark("test") as active:
            assert active is session
            raise RuntimeError("boom")
    session.stop.assert_called_once()


def test_s3_client_returns_client_after_bucket_probe(monkeypatch):
    client = MagicMock()
    factory = MagicMock(return_value=client)
    monkeypatch.setattr(s3_client.boto3, "client", factory)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    assert s3_client.get_s3_client() is client
    client.list_buckets.assert_called_once()
    assert factory.call_args.kwargs["endpoint_url"] == "http://minio:9000"


def test_s3_client_returns_none_for_known_connection_errors(monkeypatch):
    client = MagicMock()
    client.list_buckets.side_effect = EndpointConnectionError(endpoint_url="http://offline")
    monkeypatch.setattr(s3_client.boto3, "client", lambda *args, **kwargs: client)
    assert s3_client.get_s3_client() is None


def test_transfer_configuration_is_16_mib_and_four_threads():
    assert s3_transfer.MULTIPART_CHUNK_SIZE == 16 * 1024 * 1024
    assert s3_transfer.S3_TRANSFER_CONFIG.multipart_threshold == 16 * 1024 * 1024
    assert s3_transfer.S3_TRANSFER_CONFIG.multipart_chunksize == 16 * 1024 * 1024
    assert s3_transfer.S3_TRANSFER_CONFIG.max_concurrency == 4
    assert s3_transfer.S3_TRANSFER_CONFIG.use_threads is True


def test_spark_package_contract_contains_all_connectors():
    packages = spark_session.SPARK_PACKAGES
    assert "delta-spark_2.12:3.1.0" in packages
    assert "hadoop-aws:3.3.4" in packages
    assert "mysql-connector-j:8.0.33" in packages
    assert "postgresql:42.7.3" in packages

