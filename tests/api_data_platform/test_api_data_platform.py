from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from api_data_platform import data_store, main


def test_read_dataset_uses_half_open_incremental_window(monkeypatch):
    rows = [
        {"id": 1, "updated_at": "2026-01-01T00:00:00Z"},
        {"id": 2, "updated_at": "2026-01-02T00:00:00Z"},
        {"id": 3, "updated_at": "2026-01-03T00:00:00Z"},
    ]
    monkeypatch.setattr(data_store, "legacy_rows", lambda dataset: rows)

    result = data_store.read_dataset(
        "customer_reviews",
        datetime(2026, 1, 2, tzinfo=timezone.utc),
        datetime(2026, 1, 3, tzinfo=timezone.utc),
    )

    assert [row["id"] for row in result] == [2]


def test_utc_datetime_treats_naive_values_as_utc():
    parsed = data_store._utc_datetime("2026-01-02T03:04:05")
    assert parsed.tzinfo == timezone.utc
    assert parsed.hour == 3


@pytest.mark.parametrize(
    "payload,message",
    [
        ({"run_id": "", "datasets": {"customer_reviews": [{}]}}, "run_id"),
        ({"run_id": "run", "datasets": {}}, "datasets"),
        ({"run_id": "run", "datasets": {"unknown": [{}]}}, "datasets"),
        ({"run_id": "run", "datasets": {"customer_reviews": {}}}, "lista"),
    ],
)
def test_append_batch_rejects_invalid_envelopes(payload, message):
    with pytest.raises(ValueError, match=message):
        data_store.append_batch(payload)


def test_business_routes_delegate_filters(monkeypatch):
    calls = []

    def fake_read(dataset, lower, upper):
        calls.append((dataset, lower, upper))
        return [{"ok": True}]

    monkeypatch.setattr(main, "read_dataset", fake_read)
    client = TestClient(main.app)
    response = client.get(
        "/customer-reviews",
        params={
            "updated_at_from": "2026-01-01T00:00:00Z",
            "updated_at_until": "2026-02-01T00:00:00Z",
        },
    )

    assert response.status_code == 200
    assert response.json() == [{"ok": True}]
    assert calls[0][0] == "customer_reviews"
    assert calls[0][1].tzinfo is not None


def test_metrics_endpoint_is_exposed_but_hidden_from_openapi():
    client = TestClient(main.app)
    assert client.get("/metrics").status_code == 200
    assert "/metrics" not in client.get("/openapi.json").json()["paths"]

