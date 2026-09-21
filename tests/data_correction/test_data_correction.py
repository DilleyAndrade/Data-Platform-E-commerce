from datetime import date, datetime
import importlib
from unittest.mock import MagicMock

import pytest

correction = importlib.import_module("data_correction.data_correction")


def test_quarantine_listing_paginates_and_ignores_directory_markers():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"Contents": [{"Key": "b/file.csv"}, {"Key": "dir/"}], "IsTruncated": True, "NextContinuationToken": "n"},
        {"Contents": [{"Key": "a/file.csv"}], "IsTruncated": False},
    ]
    assert correction._list_quarantine_objects(client) == ["a/file.csv", "b/file.csv"]


def test_event_calculates_duration_and_preserves_counts():
    start, end = datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 0, 5)
    event = correction._event(
        "correction", "original", "orders", "mysql", "orders.parquet", "in", "out",
        "NORMALIZE_SCHEMA", 10, 8, 2, 0, 1, "PARTIAL", None, start, end, date(2026, 1, 1),
    )
    assert event["duration_seconds"] == 5
    assert event["records_corrected"] == 8
    assert event["records_still_invalid"] == 2


def test_unsupported_format_is_rejected():
    with pytest.raises(ValueError, match="Unsupported file format"):
        correction._read_dataframe(MagicMock(), "path", "xml")
    dataframe = MagicMock()
    dataframe.write.mode.return_value = MagicMock()
    with pytest.raises(ValueError, match="Unsupported file format"):
        correction._write_corrected_dataframe(dataframe, "path", "xml")


def test_attempt_falls_back_to_one_when_log_is_unavailable():
    spark = MagicMock()
    spark.read.format.side_effect = RuntimeError("offline")
    assert correction._correction_attempt(spark, "s3://quarantine/x") == 1


def test_main_function_validates_dependencies():
    with pytest.raises(ValueError, match="Spark session"):
        correction.data_correction(None, "run", date.today(), MagicMock())
    with pytest.raises(ConnectionError, match="S3/MinIO"):
        correction.data_correction(MagicMock(), "run", date.today(), None)
