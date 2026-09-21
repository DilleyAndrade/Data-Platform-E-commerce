from datetime import datetime
from unittest.mock import MagicMock

import pytest
from prometheus_client import generate_latest

from observability import obs_data_correction_log, obs_ingestion_log
from observability import obs_landing_quality_log, obs_transformation_log
from observability import prometheus_metrics


def test_ingestion_event_calculates_duration():
    start, end = datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 0, 3)
    event = obs_ingestion_log.create_ingestion_log(
        "run", "api", "reviews", "api", "reviews.json", "s3://landing", start, end,
        "SUCCESS", "",
    )
    assert event["duration_seconds"] == 3
    assert event["execution_status"] == "SUCCESS"


@pytest.mark.parametrize(
    "writer",
    [
        obs_ingestion_log.write_ingestion_log,
        obs_landing_quality_log.write_landing_quality_log,
        obs_data_correction_log.write_data_correction_log,
        obs_transformation_log.write_transformation_log,
    ],
)
def test_delta_writers_ignore_empty_event_lists(writer):
    assert writer(MagicMock(), []) is None


def test_events_total_treats_missing_and_none_as_zero():
    events = [{"records": 2}, {"records": None}, {}]
    assert prometheus_metrics._events_total(events, "records") == 2.0


@pytest.mark.parametrize("raw,expected", [(None, 3600.0), ("bad", 3600.0), ("0", 3600.0), ("42", 42.0)])
def test_pipeline_sla_validation(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("PIPELINE_SLA_SECONDS", raising=False)
    else:
        monkeypatch.setenv("PIPELINE_SLA_SECONDS", raw)
    assert prometheus_metrics.pipeline_sla_seconds() == expected


def test_sla_is_violated_only_when_strictly_greater():
    assert not prometheus_metrics.duration_violates_sla(10, 10)
    assert prometheus_metrics.duration_violates_sla(10.01, 10)


def test_push_is_skipped_without_gateway(monkeypatch):
    monkeypatch.setattr(prometheus_metrics, "load_dotenv", lambda: None)
    monkeypatch.delenv("PROMETHEUS_PUSHGATEWAY_URL", raising=False)
    push = MagicMock()
    monkeypatch.setattr(prometheus_metrics, "pushadd_to_gateway", push)
    prometheus_metrics.push_pipeline_metrics(True, 1.0)
    push.assert_not_called()


def test_push_contains_status_stage_and_quality_metrics(monkeypatch):
    monkeypatch.setenv("PROMETHEUS_PUSHGATEWAY_URL", "http://gateway:9091")
    captured = {}

    def fake_push(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(prometheus_metrics, "pushadd_to_gateway", fake_push)
    prometheus_metrics.push_pipeline_metrics(
        True,
        12.5,
        stage_events={"silver": [{"duration_seconds": 2, "records_input": 10, "records_output": 9, "records_rejected": 1, "status": "SUCCESS"}]},
        quality_events=[{"check_type": "schema", "check_status": "FAIL"}],
    )
    payload = generate_latest(captured["registry"]).decode()
    assert "data_pipeline_last_status 1.0" in payload
    assert 'data_pipeline_records_input{stage="silver"} 10.0' in payload
    assert 'data_pipeline_quality_checks_failed{check_type="schema"} 1.0' in payload
    assert "data_pipeline_quality_ratio 0.9" in payload
    assert captured["timeout"] == 10
