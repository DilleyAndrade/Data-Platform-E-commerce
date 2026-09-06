import os
from time import time
from typing import Any, Iterable, Mapping

from dotenv import load_dotenv
from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

from utils.logger import log

DEFAULT_PIPELINE_SLA_SECONDS = 3600.0


def _events_total(events: Iterable[Mapping[str, Any]], field: str) -> float:
    return sum(float(event.get(field) or 0) for event in events)


def pipeline_sla_seconds() -> float:
    raw_value = os.getenv("PIPELINE_SLA_SECONDS")
    if raw_value is None:
        return DEFAULT_PIPELINE_SLA_SECONDS
    try:
        value = float(raw_value)
    except ValueError:
        log.warning(
            "Invalid PIPELINE_SLA_SECONDS=%r; using %.0f.",
            raw_value,
            DEFAULT_PIPELINE_SLA_SECONDS,
        )
        return DEFAULT_PIPELINE_SLA_SECONDS
    if value <= 0:
        log.warning(
            "PIPELINE_SLA_SECONDS must be positive; using %.0f.",
            DEFAULT_PIPELINE_SLA_SECONDS,
        )
        return DEFAULT_PIPELINE_SLA_SECONDS
    return value


def duration_violates_sla(duration_seconds: float, sla_seconds: float) -> bool:
    return duration_seconds > sla_seconds


def _publish_stage_metrics(
    registry: CollectorRegistry,
    stage_events: Mapping[str, list[dict[str, Any]]],
) -> None:
    stage_duration = Gauge(
        "data_pipeline_stage_duration_seconds",
        "Sum of event durations in the latest pipeline run, by stage.",
        ("stage",),
        registry=registry,
    )
    records_input = Gauge(
        "data_pipeline_records_input",
        "Records read in the latest pipeline run, by stage.",
        ("stage",),
        registry=registry,
    )
    records_output = Gauge(
        "data_pipeline_records_output",
        "Records written in the latest pipeline run, by stage.",
        ("stage",),
        registry=registry,
    )
    records_rejected = Gauge(
        "data_pipeline_records_rejected",
        "Records rejected in the latest pipeline run, by stage.",
        ("stage",),
        registry=registry,
    )
    stage_failures = Gauge(
        "data_pipeline_stage_failures",
        "Failed events in the latest pipeline run, by stage.",
        ("stage",),
        registry=registry,
    )

    for stage, events in stage_events.items():
        stage_duration.labels(stage=stage).set(_events_total(events, "duration_seconds"))
        records_input.labels(stage=stage).set(_events_total(events, "records_input"))
        records_output.labels(stage=stage).set(_events_total(events, "records_output"))
        records_rejected.labels(stage=stage).set(
            _events_total(events, "records_rejected")
        )
        failures = sum(
            str(event.get("status", event.get("execution_status", ""))).upper()
            == "FAILED"
            for event in events
        )
        stage_failures.labels(stage=stage).set(failures)


def _publish_quality_metrics(
    registry: CollectorRegistry,
    quality_events: list[dict[str, Any]],
    silver_events: list[dict[str, Any]],
) -> None:
    checks_failed = Gauge(
        "data_pipeline_quality_checks_failed",
        "Quality checks that failed in the latest run, by check type.",
        ("check_type",),
        registry=registry,
    )
    failures_by_type: dict[str, int] = {}
    for event in quality_events:
        check_type = str(event.get("check_type") or "unknown")
        failures_by_type.setdefault(check_type, 0)
        if str(event.get("check_status", "")).upper() == "FAIL":
            failures_by_type[check_type] += 1
    for check_type, failures in failures_by_type.items():
        checks_failed.labels(check_type=check_type).set(failures)

    silver_input = _events_total(silver_events, "records_input")
    silver_rejected = _events_total(silver_events, "records_rejected")
    quality_ratio = 0.0
    if silver_input:
        quality_ratio = max(0.0, (silver_input - silver_rejected) / silver_input)

    Gauge(
        "data_pipeline_quality_ratio",
        "Accepted Silver records divided by Silver input records in the latest run.",
        registry=registry,
    ).set(quality_ratio)
    Gauge(
        "data_pipeline_quality_records_invalid",
        "Invalid or rejected records observed in the latest Silver run.",
        registry=registry,
    ).set(silver_rejected)


def push_pipeline_metrics(
    succeeded: bool,
    duration_seconds: float,
    stage_events: Mapping[str, list[dict[str, Any]]] | None = None,
    quality_events: list[dict[str, Any]] | None = None,
) -> None:
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
    sla_seconds = pipeline_sla_seconds()
    Gauge(
        "data_pipeline_duration_sla_seconds",
        "Configured maximum duration for the complete pipeline.",
        registry=registry,
    ).set(sla_seconds)
    Gauge(
        "data_pipeline_duration_sla_violation",
        "Whether the latest pipeline duration violated its SLA: 1 yes, 0 no.",
        registry=registry,
    ).set(1 if duration_violates_sla(duration_seconds, sla_seconds) else 0)
    completed_at = time()
    Gauge(
        "data_pipeline_last_completion_timestamp_seconds",
        "Unix timestamp of the latest completed pipeline run.",
        registry=registry,
    ).set(completed_at)

    collected_stage_events = stage_events or {}
    _publish_stage_metrics(registry, collected_stage_events)
    _publish_quality_metrics(
        registry,
        quality_events or [],
        collected_stage_events.get("silver", []),
    )

    if succeeded:
        last_success = Gauge(
            "data_pipeline_last_success_timestamp_seconds",
            "Unix timestamp of the last successfully completed pipeline.",
            registry=registry,
        )
        last_success.set(completed_at)
    else:
        last_failure = Gauge(
            "data_pipeline_last_failure_timestamp_seconds",
            "Unix timestamp of the last pipeline failure.",
            registry=registry,
        )
        last_failure.set(completed_at)

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
