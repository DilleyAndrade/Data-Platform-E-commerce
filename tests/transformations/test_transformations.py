from datetime import date, datetime
import importlib
from unittest.mock import MagicMock

import pytest

from transformation import transformation_raw_bronze as public_bronze
from transformation import transform_bronze_silver as public_silver
from transformation import transform_silver_gold as public_gold

bronze = importlib.import_module("transformation.transformation_raw_bronze")
silver = importlib.import_module("transformation.transform_bronze_silver")
gold = importlib.import_module("transformation.transform_silver_gold")


def test_public_exports_are_callable():
    assert callable(public_bronze) and callable(public_silver) and callable(public_gold)
    assert bronze.transformation_raw_bronze is public_bronze
    assert silver.transform_bronze_silver is public_silver
    assert gold.transform_silver_gold is public_gold


def test_bronze_requires_spark_s3_and_date():
    with pytest.raises(ValueError, match="Spark session"):
        bronze.transformation_raw_bronze(None, "run", date.today(), MagicMock())
    with pytest.raises(ConnectionError, match="S3/MinIO"):
        bronze.transformation_raw_bronze(MagicMock(), "run", date.today(), None)
    with pytest.raises(TypeError, match="execution_date"):
        bronze.transformation_raw_bronze(MagicMock(), "run", "2026-01-01", MagicMock())


def test_raw_prefix_exists_uses_expected_bucket():
    client = MagicMock()
    client.list_objects_v2.return_value = {"Contents": [{"Key": "x"}]}
    assert bronze._raw_prefix_exists(client, "orders/date/") is True
    assert client.list_objects_v2.call_args.kwargs["Bucket"] == "raw"


def test_bronze_event_quality_and_counts():
    start, end = datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 0, 2)
    event = bronze._transformation_event(
        "run", "orders", "raw", "bronze", 5, 5, 0, "PASS", start, end,
        "SUCCESS", None, date(2026, 1, 1),
    )
    assert event["stage"] == "bronze"
    assert event["duration_seconds"] == 2
    assert event["records_inserted"] == 5


def test_silver_contract_maps_cover_all_datasets():
    assert len(silver.SILVER_DATASETS) == 13
    assert set(silver.PRIMARY_KEYS) == set(silver.SILVER_DATASETS)
    assert 0 < silver.MINIMUM_COMPLETENESS <= 1
    for dataset, relations in silver.FOREIGN_KEYS.items():
        assert dataset in silver.SILVER_DATASETS
        assert all(parent in silver.SILVER_DATASETS for _, parent, _ in relations)


def test_silver_event_marks_warning_when_records_rejected():
    start, end = datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 0, 1)
    event = silver._transformation_event(
        "run", "orders", "s3a://bronze/orders", "s3a://silver/orders",
        10, 8, 2, 5, 3, start, end, "SUCCESS", None, date(2026, 1, 1),
    )
    assert event["data_quality_status"] == "WARNING"
    assert event["source_path"].startswith("s3://")


def test_gold_catalog_has_expected_datamarts_and_tables():
    assert len(gold.DATAMARTS) == 8
    assert len(set(gold.TABLE_GRAINS) | {"executive_kpis"}) == 27
    assert set(gold.ANOMALY_METRICS) <= set(gold.TABLE_GRAINS) | {"executive_kpis"}
    for config in gold.DATAMARTS.values():
        assert config["required"]
        assert callable(config["builder"])


def test_gold_paths_and_validation_failure_events():
    assert gold._silver_path("orders") == "s3a://silver/orders"
    assert gold._gold_path("sales", "fact_sales") == "s3a://gold/sales/fact_sales"
    events = gold._gold_validation_failure_events(
        "run", "sales", {"b": object(), "a": object()},
        ["DUPLICATE_GRAIN:fact_sales:order_item_id"], date(2026, 1, 1),
    )
    assert events[0]["validation_type"] == "DUPLICATE_GRAIN"
    assert events[0]["candidate_tables"] == "a,b"


def test_gold_event_uses_warning_for_anomaly():
    now = datetime(2026, 1, 1)
    event = gold._transformation_event(
        "run", "sales", "sales_daily", ["orders"], 10, 2, now, now,
        "SUCCESS", "ANOMALY:gross_revenue", date(2026, 1, 1),
    )
    assert event["data_quality_status"] == "WARNING"
    assert event["target_table"] == "sales.sales_daily"
