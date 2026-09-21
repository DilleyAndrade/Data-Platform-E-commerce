from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os

from delta.tables import DeltaTable
from pyspark.sql import functions as spark_functions

from path_constants.path_constants import BUCKET_OBS
from schemas.schemas import ingestion_watermark_schema
from utils.logger import log


WATERMARK_PATH = f"s3a://{BUCKET_OBS}/ingestion_watermarks"
DEFAULT_INITIAL_WATERMARK = "1970-01-01T00:00:00Z"
DEFAULT_OVERLAP_MINUTES = 10
INCREMENTAL_DATASETS = {
    "local": ("coupons", "delivery_tracking", "payments", "website_events"),
    "postgres": ("customers", "products", "suppliers"),
    "mysql": ("inventory", "order_items", "orders"),
    "api": ("customer_review", "exchange_rates", "marketing_campaigns"),
}


@dataclass(frozen=True)
class IncrementalWindow:
    source_name: str
    dataset_name: str
    lower_bound: datetime
    upper_bound: datetime
    committed_watermark: datetime | None

    def candidate(self) -> dict:
        return {
            "source_name": self.source_name,
            "dataset_name": self.dataset_name,
            "watermark_ts": self.upper_bound,
        }


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def configured_watermark_upper_bound():
    value = os.getenv("PIPELINE_WATERMARK_UNTIL")
    return _parse_utc(value) if value else None


def _initial_watermark() -> datetime:
    return _parse_utc(
        os.getenv("INCREMENTAL_INITIAL_WATERMARK", DEFAULT_INITIAL_WATERMARK)
    )


def _overlap() -> timedelta:
    minutes = int(
        os.getenv("INCREMENTAL_OVERLAP_MINUTES", str(DEFAULT_OVERLAP_MINUTES))
    )
    if minutes < 0:
        raise ValueError("INCREMENTAL_OVERLAP_MINUTES cannot be negative.")
    return timedelta(minutes=minutes)


def read_committed_watermark(spark, source_name, dataset_name):
    if not DeltaTable.isDeltaTable(spark, WATERMARK_PATH):
        return None
    rows = (
        spark.read.format("delta")
        .load(WATERMARK_PATH)
        .filter(spark_functions.col("source_name") == source_name)
        .filter(spark_functions.col("dataset_name") == dataset_name)
        .select("watermark_ts")
        .limit(1)
        .collect()
    )
    return rows[0]["watermark_ts"] if rows else None


def incremental_window(
    spark,
    source_name,
    dataset_name,
    upper_bound=None,
):
    committed = read_committed_watermark(spark, source_name, dataset_name)
    initial = _initial_watermark()
    lower_bound = initial if committed is None else max(initial, committed - _overlap())
    effective_upper = upper_bound or utc_now()
    if effective_upper <= lower_bound:
        raise ValueError(
            f"Invalid incremental window for {source_name}.{dataset_name}: "
            f"{lower_bound} >= {effective_upper}."
        )
    window = IncrementalWindow(
        source_name=source_name,
        dataset_name=dataset_name,
        lower_bound=lower_bound,
        upper_bound=effective_upper,
        committed_watermark=committed,
    )
    log.info(
        "Incremental window: source=%s dataset=%s from=%s until=%s "
        "committed_watermark=%s.",
        source_name,
        dataset_name,
        window.lower_bound.isoformat(),
        window.upper_bound.isoformat(),
        committed.isoformat() if committed else None,
    )
    return window


def attach_watermark_candidate(event, window):
    event["_watermark_candidate"] = window.candidate()
    return event


def successful_events_for_all_datasets(watermark_ts):
    return [
        {
            "execution_status": "SUCCESS",
            "_watermark_candidate": {
                "source_name": source_name,
                "dataset_name": dataset_name,
                "watermark_ts": watermark_ts,
            },
        }
        for source_name, datasets in INCREMENTAL_DATASETS.items()
        for dataset_name in datasets
    ]


def commit_watermarks(spark, ingestion_events, run_id):
    candidates = {}
    for event in ingestion_events:
        if str(event.get("execution_status", "")).upper() != "SUCCESS":
            continue
        candidate = event.get("_watermark_candidate")
        if not candidate:
            continue
        identity = (candidate["source_name"], candidate["dataset_name"])
        current = candidates.get(identity)
        if current is None or candidate["watermark_ts"] > current["watermark_ts"]:
            candidates[identity] = candidate

    if not candidates:
        return 0

    committed_at = utc_now()
    rows = [
        {
            **candidate,
            "last_successful_run_id": run_id,
            "committed_at": committed_at,
        }
        for candidate in candidates.values()
    ]
    dataframe = spark.createDataFrame(rows, schema=ingestion_watermark_schema)
    if not DeltaTable.isDeltaTable(spark, WATERMARK_PATH):
        dataframe.write.format("delta").mode("overwrite").save(WATERMARK_PATH)
        log.info("Committed %s ingestion watermarks for run_id=%s.", len(rows), run_id)
        return len(rows)

    (
        DeltaTable.forPath(spark, WATERMARK_PATH)
        .alias("target")
        .merge(
            dataframe.alias("source"),
            "target.source_name = source.source_name AND "
            "target.dataset_name = source.dataset_name",
        )
        .whenMatchedUpdateAll(
            condition="source.watermark_ts >= target.watermark_ts"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info("Committed %s ingestion watermarks for run_id=%s.", len(rows), run_id)
    return len(rows)
