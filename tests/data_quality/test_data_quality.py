from unittest.mock import MagicMock

import pytest

from data_quality import dq_landing_raw as quality


def test_quality_event_has_expected_contract():
    config = {"file_name": "x.csv", "source_name": "local"}
    event = quality._quality_event("run", config, "s3://x", "exists", "file", "PASS", total=3, valid=3)
    assert event["run_id"] == "run"
    assert event["records_total"] == 3
    assert event["check_status"] == "PASS"
    assert event["execution_ts"] is not None


def test_prefix_listing_handles_pagination():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"Contents": [{"Key": "a"}], "IsTruncated": True, "NextContinuationToken": "next"},
        {"Contents": [{"Key": "b"}], "IsTruncated": False},
    ]
    assert quality._list_prefix_objects(client, "prefix/") == ["a", "b"]
    assert client.list_objects_v2.call_args_list[1].kwargs["ContinuationToken"] == "next"


def test_partitioned_dataset_requires_parquet_part(monkeypatch):
    monkeypatch.setattr(quality, "_list_prefix_objects", lambda *args: ["customers/_SUCCESS"])
    with pytest.raises(FileNotFoundError, match="No Parquet parts"):
        quality._find_landing_objects(MagicMock(), "customers", "customers/date/")


def test_move_requires_common_prefix():
    with pytest.raises(ValueError, match="share"):
        quality._move_landing_objects(MagicMock(), ["a/x.csv", "b/y.csv"], "raw")


def test_move_deletes_destination_then_copies_then_removes_source(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(quality, "_list_prefix_objects", lambda *args: ["x/date/old.csv"])
    quality._move_landing_objects(client, ["x/date/new.csv"], "raw")
    client.copy.assert_called_once()
    deletes = client.delete_object.call_args_list
    assert deletes[0].kwargs == {"Bucket": "raw", "Key": "x/date/old.csv"}
    assert deletes[1].kwargs == {"Bucket": "landing", "Key": "x/date/new.csv"}


def test_routing_uses_quarantine_when_any_check_failed(monkeypatch):
    destinations = []
    monkeypatch.setattr(
        quality, "_move_landing_objects",
        lambda client, keys, destination: destinations.append(destination),
    )
    event = quality._route_validated_object(
        MagicMock(), ["x/date/x.csv"], "x/date/", {"file_name": "x.csv", "source_name": "local"},
        "run", [{"check_status": "FAIL"}], 10,
    )
    assert destinations == ["quarantine"]
    assert event["check_name"] == "route_to_quarantine"
    assert event["check_status"] == "PASS"

