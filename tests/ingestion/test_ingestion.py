from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ingestion import ingestion_api, ingestion_local, ingestion_mysql, ingestion_postgres
from ingestion import incremental_state


def test_local_discovery_filters_and_sorts(tmp_path):
    (tmp_path / "b.json").write_text("[]", encoding="utf-8")
    (tmp_path / "A.csv").write_text("id\n", encoding="utf-8")
    (tmp_path / "ignored.txt").write_text("x", encoding="utf-8")
    assert [p.name for p in ingestion_local.discover_local_files(tmp_path)] == ["A.csv", "b.json"]


def test_local_discovery_requires_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        ingestion_local.discover_local_files(tmp_path / "missing")
    file_path = tmp_path / "file.csv"
    file_path.write_text("x", encoding="utf-8")
    with pytest.raises(NotADirectoryError):
        ingestion_local.discover_local_files(file_path)


def test_incremental_csv_payload_uses_half_open_window(tmp_path):
    path = tmp_path / "events.csv"
    path.write_text(
        "id,updated_at\n1,2026-01-01T00:00:00Z\n2,2026-01-02T00:00:00Z\n3,2026-01-03T00:00:00Z\n",
        encoding="utf-8",
    )
    payload = ingestion_local._incremental_payload(
        path, datetime(2026, 1, 2), datetime(2026, 1, 3)
    ).decode()
    assert "2,2026-01-02" in payload
    assert "1,2026-01-01" not in payload
    assert "3,2026-01-03" not in payload


def test_landing_locations_are_partitioned_by_date():
    key, path = ingestion_local.build_landing_location(Path("payments.csv"), date(2026, 9, 20))
    assert key == "payments/ingestion_date_20260920/payments.csv"
    assert path == "s3://landing/payments/ingestion_date_20260920/"
    api_key, _ = ingestion_api.build_landing_location("exchange_rates", date(2026, 9, 20))
    assert api_key.endswith("exchange_rates.json")


def test_api_stream_sends_incremental_parameters_and_uploads():
    response = MagicMock()
    response.raw = MagicMock()
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    session = MagicMock()
    session.get.return_value = response
    s3 = MagicMock()

    result = ingestion_api.stream_api_to_s3(
        session, s3, "http://api/data", "dataset", date(2026, 1, 2),
        datetime(2026, 1, 1), datetime(2026, 1, 2),
    )

    assert result == "s3://landing/dataset/ingestion_date_20260102/"
    params = session.get.call_args.kwargs["params"]
    assert params["updated_at_from"].endswith("Z")
    assert params["updated_at_until"].endswith("Z")
    response.raise_for_status.assert_called_once()
    s3.upload_fileobj.assert_called_once()


def test_database_incremental_queries_are_half_open():
    lower, upper = datetime(2026, 1, 1), datetime(2026, 2, 1)
    pg = ingestion_postgres._postgres_incremental_source("customers", lower, upper)
    mysql = ingestion_mysql._mysql_incremental_source("orders", lower, upper)
    assert "updated_at >=" in pg and "updated_at <" in pg
    assert "updated_at >=" in mysql and "updated_at <" in mysql
    assert "public.customers" in pg


def test_database_config_reports_missing_settings(monkeypatch):
    monkeypatch.setattr(ingestion_postgres, "load_dotenv", lambda: None)
    for name in ("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="Missing required PostgreSQL"):
        ingestion_postgres.create_postgres_jdbc_config()


def test_incremental_window_applies_overlap(monkeypatch):
    monkeypatch.setenv("INCREMENTAL_INITIAL_WATERMARK", "2020-01-01T00:00:00Z")
    monkeypatch.setenv("INCREMENTAL_OVERLAP_MINUTES", "10")
    monkeypatch.setattr(
        incremental_state, "read_committed_watermark",
        lambda spark, source, dataset: datetime(2026, 1, 2, 0, 0),
    )
    window = incremental_state.incremental_window(
        object(), "api", "dataset", datetime(2026, 1, 3)
    )
    assert window.lower_bound == datetime(2026, 1, 1, 23, 50)
    assert window.candidate()["watermark_ts"] == datetime(2026, 1, 3)


def test_incremental_window_rejects_invalid_bounds(monkeypatch):
    monkeypatch.setattr(
        incremental_state, "read_committed_watermark",
        lambda *args: datetime(2026, 1, 3),
    )
    with pytest.raises(ValueError, match="Invalid incremental window"):
        incremental_state.incremental_window(object(), "api", "x", datetime(2026, 1, 2))


def test_all_incremental_datasets_receive_commit_candidates():
    events = incremental_state.successful_events_for_all_datasets(datetime(2026, 1, 1))
    expected = sum(len(items) for items in incremental_state.INCREMENTAL_DATASETS.values())
    assert len(events) == expected == 13
    assert all(event["execution_status"] == "SUCCESS" for event in events)
