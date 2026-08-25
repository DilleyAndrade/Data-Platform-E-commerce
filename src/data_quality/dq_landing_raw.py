from datetime import datetime, timezone
from functools import reduce
from pyspark.sql import functions as spark_functions
from observability.obs_landing_quality_log import write_landing_quality_log
from path_constants.path_constants import BUCKET_LAN, BUCKET_QUA, BUCKET_RAW
from utils.logger import log
from utils.s3_transfer import S3_TRANSFER_CONFIG


LANDING_DATASETS = {
    "coupons": {"source_name": "local", "file_name": "coupons.csv", "format": "csv", "required_columns": ["coupon", "discount", "start_date", "end_date"], "required_fields": ["coupon"]},
    "delivery_tracking": {"source_name": "local", "file_name": "delivery_tracking.csv", "format": "csv", "required_columns": ["tracking_id", "order_id", "status", "updated_at"], "required_fields": ["tracking_id", "order_id"]},
    "payments": {"source_name": "local", "file_name": "payments.csv", "format": "csv", "required_columns": ["payment_id", "order_id", "payment_method", "payment_status", "amount"], "required_fields": ["payment_id", "order_id"]},
    "website_events": {"source_name": "local", "file_name": "website_events.json", "format": "json", "required_columns": ["event_id", "customer_id", "event", "page", "timestamp"], "required_fields": ["event_id", "customer_id"]},
    "customer_review": {"source_name": "api", "file_name": "customer_review.json", "format": "json", "required_columns": ["review_id", "customer_id", "product_id", "rating", "comment"], "required_fields": ["review_id", "customer_id", "product_id"]},
    "exchange_rates": {"source_name": "api", "file_name": "exchange_rates.json", "format": "json", "required_columns": ["date", "usd_brl", "eur_brl"], "required_fields": ["date"]},
    "marketing_campaigns": {"source_name": "api", "file_name": "marketing_campaigns.json", "format": "json", "required_columns": ["campaign_id", "campaign", "channel", "budget"], "required_fields": ["campaign_id"]},
    "customers": {"source_name": "postgres", "file_name": "customers.parquet", "format": "parquet", "required_columns": [], "required_fields": []},
    "products": {"source_name": "postgres", "file_name": "products.parquet", "format": "parquet", "required_columns": [], "required_fields": []},
    "suppliers": {"source_name": "postgres", "file_name": "suppliers.parquet", "format": "parquet", "required_columns": [], "required_fields": []},
    "inventory": {"source_name": "mysql", "file_name": "inventory.parquet", "format": "parquet", "required_columns": [], "required_fields": []},
    "order_items": {"source_name": "mysql", "file_name": "order_items.parquet", "format": "parquet", "required_columns": [], "required_fields": []},
    "orders": {"source_name": "mysql", "file_name": "orders.parquet", "format": "parquet", "required_columns": [], "required_fields": []},
}

PARTITIONED_DATASETS = {
    "customers",
    "products",
    "suppliers",
    "inventory",
    "order_items",
    "orders",
}


def _execution_timestamp():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _quality_event(
    run_id,
    config,
    file_path,
    check_name,
    check_type,
    status,
    total=0,
    valid=0,
    invalid=0,
    invalid_percentage=0.0,
    error_message=None,
):
    return {
        "run_id": run_id,
        "file_name": config["file_name"],
        "source_name": config["source_name"],
        "file_path": file_path,
        "check_name": check_name,
        "check_type": check_type,
        "records_total": total,
        "records_valid": valid,
        "records_invalid": invalid,
        "invalid_percentage": invalid_percentage,
        "check_status": status,
        "error_message": error_message,
        "execution_ts": _execution_timestamp(),
    }


def _read_spark_dataframe(spark, file_path, file_format):
    if file_format == "csv":
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(file_path)
        )
    if file_format == "json":
        return spark.read.option("multiLine", True).json(file_path)
    if file_format == "parquet":
        return spark.read.parquet(file_path)
    raise ValueError(f"Unsupported file format: {file_format}")


def _list_prefix_objects(s3_client, prefix):
    keys = []
    continuation_token = None

    while True:
        request = {"Bucket": BUCKET_LAN, "Prefix": prefix}
        if continuation_token is not None:
            request["ContinuationToken"] = continuation_token
        response = s3_client.list_objects_v2(**request)
        keys.extend(item["Key"] for item in response.get("Contents", []))

        if not response.get("IsTruncated"):
            return keys
        continuation_token = response["NextContinuationToken"]


def _find_landing_objects(s3_client, dataset, key_or_prefix):
    if dataset in PARTITIONED_DATASETS:
        keys = _list_prefix_objects(s3_client, key_or_prefix)
        if not any(key.endswith(".parquet") for key in keys):
            raise FileNotFoundError(
                f"No Parquet parts found under landing/{key_or_prefix}"
            )
        return keys

    s3_client.head_object(Bucket=BUCKET_LAN, Key=key_or_prefix)
    return [key_or_prefix]


def _move_landing_objects(s3_client, keys, destination_bucket):
    for key in keys:
        s3_client.copy(
            {"Bucket": BUCKET_LAN, "Key": key},
            destination_bucket,
            key,
            Config=S3_TRANSFER_CONFIG,
        )

    for key in keys:
        s3_client.delete_object(Bucket=BUCKET_LAN, Key=key)


def _route_validated_object(
    s3_client,
    keys,
    destination_key,
    config,
    run_id,
    dataset_validations,
    records_total,
):
    has_quality_failure = any(
        validation["check_status"] == "FAIL"
        for validation in dataset_validations
    )
    destination_bucket = BUCKET_QUA if has_quality_failure else BUCKET_RAW
    check_name = "route_to_quarantine" if has_quality_failure else "route_to_raw"
    destination_path = f"s3://{destination_bucket}/{destination_key}"

    try:
        _move_landing_objects(s3_client, keys, destination_bucket)
        status = "PASS"
        error_message = None
    except Exception as error:
        status = "FAIL"
        error_message = f"{type(error).__name__}: {error}"
        log.exception(
            "Failed to move %s to %s.",
            destination_key,
            destination_bucket,
        )

    return _quality_event(
        run_id,
        config,
        destination_path,
        check_name,
        "routing",
        status,
        total=records_total,
        valid=records_total if status == "PASS" else 0,
        invalid=records_total if status == "FAIL" else 0,
        invalid_percentage=100.0 if status == "FAIL" and records_total else 0.0,
        error_message=error_message,
    )


def dq_landing_raw(spark, run_id, ingestion_date, s3_client):
    if spark is None:
        raise ValueError("A Spark session is required for Landing data quality.")
    if s3_client is None:
        raise ConnectionError("Could not create the S3/MinIO client for data quality.")

    partition = ingestion_date.strftime("%Y%m%d")
    validations = []

    for dataset, config in LANDING_DATASETS.items():
        dataset_validation_start = len(validations)
        directory = f"{dataset}/ingestion_date_{partition}/"
        key_or_prefix = (
            directory
            if dataset in PARTITIONED_DATASETS
            else f"{directory}{config['file_name']}"
        )
        landing_path = f"s3://{BUCKET_LAN}/{key_or_prefix}"
        spark_path = f"s3a://{BUCKET_LAN}/{key_or_prefix}"

        try:
            object_keys = _find_landing_objects(
                s3_client,
                dataset,
                key_or_prefix,
            )
            validations.append(
                _quality_event(
                    run_id,
                    config,
                    landing_path,
                    "file_exists",
                    "existence",
                    "PASS",
                )
            )
        except Exception as error:
            validations.append(
                _quality_event(
                    run_id,
                    config,
                    landing_path,
                    "file_exists",
                    "existence",
                    "FAIL",
                    error_message=f"{type(error).__name__}: {error}",
                )
            )
            continue

        try:
            dataframe = _read_spark_dataframe(spark, spark_path, config["format"])
            total = dataframe.count()
            validations.append(
                _quality_event(
                    run_id,
                    config,
                    landing_path,
                    "file_integrity",
                    "format",
                    "PASS",
                    total=total,
                    valid=total,
                )
            )
        except Exception as error:
            validations.append(
                _quality_event(
                    run_id,
                    config,
                    landing_path,
                    "file_integrity",
                    "format",
                    "FAIL",
                    error_message=f"{type(error).__name__}: {error}",
                )
            )
            validations.append(
                _route_validated_object(
                    s3_client,
                    object_keys,
                    key_or_prefix,
                    config,
                    run_id,
                    validations[dataset_validation_start:],
                    0,
                )
            )
            continue

        is_empty = total == 0
        validations.append(
            _quality_event(
                run_id,
                config,
                landing_path,
                "records_not_empty",
                "volume",
                "FAIL" if is_empty else "PASS",
                total=total,
                valid=total,
                error_message="The file contains no records." if is_empty else None,
            )
        )

        missing_columns = sorted(set(config["required_columns"]) - set(dataframe.columns))
        validations.append(
            _quality_event(
                run_id,
                config,
                landing_path,
                "required_columns",
                "schema",
                "FAIL" if missing_columns else "PASS",
                total=total,
                valid=0 if missing_columns else total,
                invalid=total if missing_columns else 0,
                invalid_percentage=100.0 if missing_columns and total else 0.0,
                error_message=(
                    f"Missing required columns: {', '.join(missing_columns)}"
                    if missing_columns
                    else None
                ),
            )
        )

        required_fields = config["required_fields"]
        if required_fields and set(required_fields).issubset(dataframe.columns):
            null_condition = reduce(
                lambda left, right: left | right,
                (spark_functions.col(field).isNull() for field in required_fields),
            )
            invalid_nulls = dataframe.filter(null_condition).count()
            validations.append(
                _quality_event(
                    run_id,
                    config,
                    landing_path,
                    "required_fields_not_null",
                    "null",
                    "FAIL" if invalid_nulls else "PASS",
                    total=total,
                    valid=total - invalid_nulls,
                    invalid=invalid_nulls,
                    invalid_percentage=(
                        round(invalid_nulls / total * 100, 2) if total else 0.0
                    ),
                    error_message=(
                        "Required fields contain null values."
                        if invalid_nulls
                        else None
                    ),
                )
            )

        duplicates = total - dataframe.dropDuplicates().count()
        validations.append(
            _quality_event(
                run_id,
                config,
                landing_path,
                "duplicate_records",
                "duplicate",
                "FAIL" if duplicates else "PASS",
                total=total,
                valid=total - duplicates,
                invalid=duplicates,
                invalid_percentage=(
                    round(duplicates / total * 100, 2) if total else 0.0
                ),
                error_message=(
                    "Duplicate records were found." if duplicates else None
                ),
            )
        )
        validations.append(
            _route_validated_object(
                s3_client,
                object_keys,
                key_or_prefix,
                config,
                run_id,
                validations[dataset_validation_start:],
                total,
            )
        )

    if validations:
        write_landing_quality_log(spark, validations)

    failures = sum(
        validation["check_status"] == "FAIL"
        for validation in validations
    )
    log.info(
        "Landing data quality finished: run_id=%s, checks=%s, failures=%s.",
        run_id,
        len(validations),
        failures,
    )
    return validations
