import os
from time import time
from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway
from utils.logger import log


def push_pipeline_metrics(succeeded: bool, duration_seconds: float) -> None:
    load_dotenv()

    gateway_url = os.getenv("PROMETHEUS_PUSHGATEWAY_URL")
    job_name = os.getenv(
        "PROMETHEUS_JOB_NAME",
        "data_platform_pipeline",
    )

    if not gateway_url:
        log.warning(
            "PROMETHEUS_PUSHGATEWAY_URL is not configured. "
            "Pipeline metrics will not be sent."
        )
        return

    registry = CollectorRegistry()

    pipeline_status = Gauge(
        "data_pipeline_last_status",
        "Status of the last pipeline execution: 1 for success and 0 for failure.",
        registry=registry,
    )
    pipeline_duration = Gauge(
        "data_pipeline_last_duration_seconds",
        "Duration of the last complete pipeline execution in seconds.",
        registry=registry,
    )

    pipeline_status.set(1 if succeeded else 0)
    pipeline_duration.set(duration_seconds)

    if succeeded:
        last_success = Gauge(
            "data_pipeline_last_success_timestamp_seconds",
            "Unix timestamp of the last successfully completed pipeline.",
            registry=registry,
        )
        last_success.set(time())
    else:
        last_failure = Gauge(
            "data_pipeline_last_failure_timestamp_seconds",
            "Unix timestamp of the last pipeline failure.",
            registry=registry,
        )
        last_failure.set(time())

    try:
        pushadd_to_gateway(
            gateway=gateway_url,
            job=job_name,
            registry=registry,
            timeout=10,
        )
        log.info(
            "Pipeline metrics sent to Pushgateway: status=%s duration_seconds=%.2f.",
            "SUCCESS" if succeeded else "FAILED",
            duration_seconds,
        )
    except Exception:
        log.warning(
            "Could not send pipeline metrics to Pushgateway at %s.",
            gateway_url,
            exc_info=True,
        )
