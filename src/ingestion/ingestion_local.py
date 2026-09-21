import csv
from datetime import date, datetime, timezone
from io import BytesIO, StringIO
import json
from pathlib import Path
from typing import Any

from observability.obs_ingestion_log import create_ingestion_log, write_ingestion_log
from ingestion.incremental_state import (
    attach_watermark_candidate,
    configured_watermark_upper_bound,
    incremental_window,
    utc_now,
)
from path_constants.path_constants import BUCKET_LAN, PATH_LOCAL_FILES
from utils.logger import log
from utils.s3_transfer import S3_TRANSFER_CONFIG

SUPPORTED_FILE_FORMATS = {".csv", ".json"}


def discover_local_files(source_directory: Path) -> list[Path]:
    if not source_directory.exists():
        raise FileNotFoundError(f"Source directory does not exist: {source_directory}")
    if not source_directory.is_dir():
        raise NotADirectoryError(f"Source path is not a directory: {source_directory}")

    supported_files = []

    for path in source_directory.iterdir():
        is_file = path.is_file()
        is_supported = path.suffix.lower() in SUPPORTED_FILE_FORMATS

        if is_file and is_supported:
            supported_files.append(path)

    supported_files.sort(key=lambda path: path.name.lower())
    return supported_files


def build_landing_location(file_path: Path, ingestion_date: date) -> tuple[str, str]:
    date_partition = ingestion_date.strftime("%Y%m%d")
    directory = f"{file_path.stem}/ingestion_date_{date_partition}"
    key = f"{directory}/{file_path.name}"
    s3_path = f"s3://{BUCKET_LAN}/{directory}/"
    return key, s3_path


def upload_local_file(
    s3_client: Any,
    file_path: Path,
    ingestion_date: date,
    lower_bound: datetime | None = None,
    upper_bound: datetime | None = None,
) -> str:
    key, s3_path = build_landing_location(file_path, ingestion_date)
    log.info("Trying to save %s in S3.", file_path.name)
    if lower_bound is None or upper_bound is None:
        s3_client.upload_file(
            Filename=str(file_path),
            Bucket=BUCKET_LAN,
            Key=key,
            Config=S3_TRANSFER_CONFIG,
        )
    else:
        payload = _incremental_payload(file_path, lower_bound, upper_bound)
        s3_client.upload_fileobj(
            BytesIO(payload),
            BUCKET_LAN,
            key,
            Config=S3_TRANSFER_CONFIG,
        )
    log.info("Successfully saved %s in S3.", file_path.name)
    return s3_path


def _row_updated_at(row):
    value = datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _incremental_payload(file_path, lower_bound, upper_bound):
    if file_path.suffix.lower() == ".json":
        rows = json.loads(file_path.read_text(encoding="utf-8-sig"))
        if not isinstance(rows, list):
            rows = [rows]
        selected = [
            row
            for row in rows
            if lower_bound <= _row_updated_at(row) < upper_bound
        ]
        return (json.dumps(selected, ensure_ascii=False) + "\n").encode("utf-8")

    with file_path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        fieldnames = reader.fieldnames
        selected = [
            row
            for row in reader
            if lower_bound <= _row_updated_at(row) < upper_bound
        ]
    output = StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(selected)
    return output.getvalue().encode("utf-8")


def ingestion_local(
    spark: Any,
    run_id: str,
    ingestion_date: date,
    s3_client: Any,
    source_directory: str | Path = PATH_LOCAL_FILES,
    watermark_upper_bound: datetime | None = None,
) -> list[dict[str, Any]]:
    log.info("Started local ingestion.")
    source_path = Path(source_directory)
    if s3_client is None:
        raise ConnectionError("Could not create the S3/MinIO client.")

    log.info("Searching for files in %s.", source_path)
    files = discover_local_files(source_path)
    ingestion_logs: list[dict[str, Any]] = []

    if not files:
        log.warning("No supported files found in %s.", source_path)

    for file_path in files:
        started_at = datetime.now()
        file_name = file_path.stem
        window = incremental_window(
            spark,
            "local",
            file_name,
            watermark_upper_bound or configured_watermark_upper_bound() or utc_now(),
        )
        file_format = file_path.suffix.lower().lstrip(".")
        _, s3_path = build_landing_location(file_path, ingestion_date)
        log.info("Reading file %s.", file_path.name)

        try:
            s3_path = upload_local_file(
                s3_client,
                file_path,
                ingestion_date,
                window.lower_bound,
                window.upper_bound,
            )
            status = "SUCCESS"
            error_message = ""
        except Exception as error:
            status = "FAILED"
            error_message = f"{type(error).__name__}: {error}"
            log.exception("Failed to ingest local file %s.", file_path)

        ended_at = datetime.now()
        event = create_ingestion_log(
                run_id,
                "local",
                file_name,
                file_format,
                file_path.name,
                s3_path,
                started_at,
                ended_at,
                status,
                error_message,
            )
        if status == "SUCCESS":
            attach_watermark_candidate(event, window)
        ingestion_logs.append(event)

    write_ingestion_log(spark, ingestion_logs)
    log.info("Finished local ingestion.")
    return ingestion_logs


if __name__ == "__main__":
    from utils.job import (
        job_arguments,
        job_spark,
        raise_for_failed_events,
        required_s3_client,
    )

    arguments = job_arguments("Ingest local files into Landing.")
    with job_spark("landing_ingestion_local") as spark_session:
        cli_events = ingestion_local(
            spark_session,
            arguments.run_id,
            arguments.execution_date,
            required_s3_client(),
        )
    raise_for_failed_events(cli_events, "Local ingestion")
