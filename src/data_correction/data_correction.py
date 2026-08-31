from datetime import date, datetime, timezone
from pathlib import PurePosixPath
from pyspark.sql import functions as spark_functions
from data_quality.dq_landing_raw import LANDING_DATASETS
from observability.obs_data_correction_log import write_data_correction_log
from path_constants.path_constants import BUCKET_OBS, BUCKET_QUA, BUCKET_RAW
from utils.logger import log


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _list_quarantine_objects(s3_client):
    keys = []
    continuation_token = None
    while True:
        if continuation_token is None:
            response = s3_client.list_objects_v2(Bucket=BUCKET_QUA)
        else:
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_QUA,
                ContinuationToken=continuation_token,
            )
        keys.extend(
            item["Key"]
            for item in response.get("Contents", [])
            if not item["Key"].endswith("/")
        )
        if not response.get("IsTruncated"):
            return sorted(keys)
        continuation_token = response["NextContinuationToken"]


def _read_dataframe(spark, path, file_format):
    if file_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if file_format == "json":
        return spark.read.option("multiLine", True).json(path)
    if file_format == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported file format: {file_format}")


def _normalize_schema(dataframe, expected_columns):
    if not expected_columns:
        return dataframe, []

    actual_columns = set(dataframe.columns)
    expected_set = set(expected_columns)
    unexpected = sorted(actual_columns - expected_set)
    missing = sorted(expected_set - actual_columns)
    corrections = []

    if unexpected:
        corrections.append("DROP_UNEXPECTED_COLUMNS")
    for column in missing:
        dataframe = dataframe.withColumn(
            column,
            spark_functions.lit(None).cast("string"),
        )
    if missing:
        corrections.append("ADD_MISSING_COLUMNS")

    return dataframe.select(expected_columns), corrections


def _count_still_invalid(dataframe, required_fields):
    invalid_nulls = 0
    if required_fields and set(required_fields).issubset(dataframe.columns):
        condition = None
        for field in required_fields:
            field_is_null = spark_functions.col(field).isNull()
            condition = (
                field_is_null if condition is None else condition | field_is_null
            )
        invalid_nulls = dataframe.filter(condition).count()

    total = dataframe.count()
    duplicates = total - dataframe.dropDuplicates().count()
    return min(total, invalid_nulls + duplicates)


def _write_corrected_dataframe(
    dataframe,
    target_path,
    file_format,
):
    writer = dataframe.write.mode("overwrite")
    if file_format == "csv":
        writer.option("header", True).csv(target_path)
    elif file_format == "json":
        writer.json(target_path)
    elif file_format == "parquet":
        writer.parquet(target_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")


def _original_run_id(spark, original_path):
    try:
        rows = (
            spark.read.format("delta")
            .load(f"s3a://{BUCKET_OBS}/landing_quality_log")
            .filter(spark_functions.col("file_path") == original_path)
            .orderBy(spark_functions.col("execution_ts").desc())
            .select("run_id")
            .limit(1)
            .collect()
        )
        return rows[0]["run_id"] if rows else None
    except Exception:
        log.warning("Could not resolve original run for %s.", original_path)
        return None


def _correction_attempt(spark, original_path):
    try:
        return 1 + (
            spark.read.format("delta")
            .load(f"s3a://{BUCKET_OBS}/data_correction_log")
            .filter(spark_functions.col("original_path") == original_path)
            .count()
        )
    except Exception:
        return 1


def _event(
    correction_run_id,
    original_run_id,
    dataset_name,
    source_name,
    file_name,
    original_path,
    target_path,
    correction_type,
    records_input,
    records_corrected,
    records_still_invalid,
    records_discarded,
    correction_attempt,
    correction_status,
    error_message,
    started_at,
    ended_at,
    execution_date,
):
    return {
        "correction_run_id": correction_run_id,
        "original_run_id": original_run_id,
        "dataset_name": dataset_name,
        "source_name": source_name,
        "file_name": file_name,
        "original_path": original_path,
        "target_path": target_path,
        "correction_type": correction_type,
        "records_input": records_input,
        "records_corrected": records_corrected,
        "records_still_invalid": records_still_invalid,
        "records_discarded": records_discarded,
        "correction_attempt": correction_attempt,
        "correction_status": correction_status,
        "error_message": error_message,
        "correction_start_ts": started_at,
        "correction_end_ts": ended_at,
        "duration_seconds": (ended_at - started_at).total_seconds(),
        "execution_date": execution_date,
    }


def data_correction(spark, correction_run_id, execution_date, s3_client):
    log.info("Data correction started.")
    if spark is None:
        raise ValueError("A Spark session is required for data correction.")
    if s3_client is None:
        raise ConnectionError(
            "Could not create the S3/MinIO client for data correction."
        )
    if not isinstance(execution_date, date):
        raise TypeError("execution_date must be a date.")

    events = []
    for key in _list_quarantine_objects(s3_client):
        started_at = _utc_now()
        dataset = key.split("/", 1)[0]
        config = LANDING_DATASETS.get(dataset)
        original_path = f"s3://{BUCKET_QUA}/{key}"
        source_path = f"s3a://{BUCKET_QUA}/{key}"
        original_run_id = _original_run_id(spark, original_path)
        attempt = _correction_attempt(spark, original_path)
        records_input = 0
        target_path = None

        try:
            if config is None:
                raise KeyError(f"No correction contract found for dataset: {dataset}")

            expected_columns = config["required_columns"]
            expected_format = config["format"]
            dataframe = _read_dataframe(
                spark,
                source_path,
                expected_format,
            )
            records_input = dataframe.count()
            dataframe, corrections = _normalize_schema(dataframe, expected_columns)

            records_still_invalid = _count_still_invalid(
                dataframe,
                config["required_fields"],
            )
            partition = execution_date.strftime("%Y%m%d")
            target_key = f"{dataset}/ingestion_date_{partition}/"
            spark_target_path = f"s3a://{BUCKET_RAW}/{target_key}"
            target_path = f"s3://{BUCKET_RAW}/{target_key}"
            _write_corrected_dataframe(
                dataframe,
                spark_target_path,
                expected_format,
            )
            status = "PARTIAL" if records_still_invalid else "SUCCESS"
            if status == "SUCCESS":
                s3_client.delete_object(Bucket=BUCKET_QUA, Key=key)
            ended_at = _utc_now()
            events.append(
                _event(
                    correction_run_id,
                    original_run_id,
                    dataset,
                    config["source_name"],
                    config["file_name"],
                    original_path,
                    target_path,
                    ",".join(corrections) or "REWRITE_EXPECTED_FORMAT",
                    records_input,
                    records_input - records_still_invalid,
                    records_still_invalid,
                    0,
                    attempt,
                    status,
                    None,
                    started_at,
                    ended_at,
                    execution_date,
                )
            )
        except Exception as error:
            ended_at = _utc_now()
            log.exception("Failed to correct quarantined object %s.", key)
            events.append(
                _event(
                    correction_run_id,
                    original_run_id,
                    dataset,
                    config["source_name"] if config else None,
                    config["file_name"] if config else PurePosixPath(key).name,
                    original_path,
                    target_path,
                    "UNDETERMINED",
                    records_input,
                    0,
                    records_input,
                    0,
                    attempt,
                    "FAILED",
                    f"{type(error).__name__}: {error}",
                    started_at,
                    ended_at,
                    execution_date,
                )
            )

    write_data_correction_log(spark, events)
    log.info("Data correction finished.")
    return events


if __name__ == "__main__":
    from utils.job import job_arguments, job_spark, required_s3_client

    arguments = job_arguments("Correct quarantined records.")
    with job_spark("data_correction") as spark_session:
        data_correction(
            spark_session,
            f"correction_{arguments.run_id}",
            arguments.execution_date,
            required_s3_client(),
        )
