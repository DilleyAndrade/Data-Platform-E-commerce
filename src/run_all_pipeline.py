import argparse
from datetime import date, datetime, timezone
from uuid import uuid4
from data_quality.dq_landing_raw import dq_landing_raw
from data_correction.data_correction import data_correction
from transformation.transformation_raw_bronze import transformation_raw_bronze
from transformation.transform_bronze_silver import transform_bronze_silver
from transformation.transform_silver_gold import transform_silver_gold
from ingestion.ingestion_api import ingestion_api
from ingestion.ingestion_local import ingestion_local
from ingestion.ingestion_mysql import ingestion_mysql
from ingestion.ingestion_postgres import ingestion_postgres
from ingestion.incremental_state import commit_watermarks, utc_now
from utils.s3_client import get_s3_client
from utils.spark_session import spark_session
from time import monotonic
from observability.prometheus_metrics import push_pipeline_metrics


def _utc_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def run_pipeline(
    run_id: str,
    ingestion_date: date,
    watermark_upper_bound: datetime | None = None,
) -> None:
    started_at = monotonic()
    succeeded = False
    spark = None
    stage_events = {
        "ingestion": [],
        "correction": [],
        "bronze": [],
        "silver": [],
        "gold": [],
    }
    quality_events = []

    try:
        spark = spark_session(
            "ingestion_pipeline",
            "local[*]",
        )

        s3_client = get_s3_client()

        if s3_client is None:
            raise ConnectionError("Could not create the S3/MinIO client.")

        effective_watermark_upper_bound = watermark_upper_bound or utc_now()

        local_events = ingestion_local(
            spark,
            run_id,
            ingestion_date,
            s3_client,
            watermark_upper_bound=effective_watermark_upper_bound,
        )
        stage_events["ingestion"].extend(local_events)
        postgres_events = ingestion_postgres(
            spark,
            run_id,
            ingestion_date,
            watermark_upper_bound=effective_watermark_upper_bound,
        )
        stage_events["ingestion"].extend(postgres_events)
        mysql_events = ingestion_mysql(
            spark,
            run_id,
            ingestion_date,
            watermark_upper_bound=effective_watermark_upper_bound,
        )
        stage_events["ingestion"].extend(mysql_events)
        api_events = ingestion_api(
            spark,
            run_id,
            ingestion_date,
            s3_client,
            watermark_upper_bound=effective_watermark_upper_bound,
        )
        stage_events["ingestion"].extend(api_events)
        quality_events = dq_landing_raw(
            spark,
            run_id,
            ingestion_date,
            s3_client,
        )
        correction_events = data_correction(
            spark,
            f"correction_{run_id}",
            ingestion_date,
            s3_client,
        )
        stage_events["correction"] = correction_events
        bronze_events = transformation_raw_bronze(
            spark,
            run_id,
            ingestion_date,
            s3_client,
        )
        stage_events["bronze"] = bronze_events
        silver_events = transform_bronze_silver(
            spark,
            run_id,
            ingestion_date,
        )
        stage_events["silver"] = silver_events
        gold_events = transform_silver_gold(
            spark,
            run_id,
            ingestion_date,
        )
        stage_events["gold"] = gold_events
        stage_failed = any(
            str(event.get("status", event.get("execution_status", ""))).upper()
            == "FAILED"
            for events in stage_events.values()
            for event in events
        )
        routing_failed = any(
            event.get("check_type") == "routing"
            and str(event.get("check_status", "")).upper() == "FAIL"
            for event in quality_events
        )
        if not stage_failed and not routing_failed:
            commit_watermarks(
                spark,
                stage_events["ingestion"],
                run_id,
            )
            succeeded = True

    finally:
        duration_seconds = monotonic() - started_at

        if spark is not None:
            spark.stop()

        push_pipeline_metrics(
            succeeded=succeeded,
            duration_seconds=duration_seconds,
            stage_events=stage_events,
            quality_events=quality_events,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the complete data pipeline.")
    parser.add_argument("--run-id", default=f"ingestion_{uuid4()}")
    parser.add_argument(
        "--date",
        dest="execution_date",
        type=date.fromisoformat,
        default=date.today(),
    )
    parser.add_argument(
        "--watermark-until",
        type=_utc_timestamp,
        default=None,
        help="Exclusive UTC upper bound for the incremental window.",
    )
    arguments = parser.parse_args()
    run_pipeline(
        run_id=arguments.run_id,
        ingestion_date=arguments.execution_date,
        watermark_upper_bound=arguments.watermark_until,
    )
