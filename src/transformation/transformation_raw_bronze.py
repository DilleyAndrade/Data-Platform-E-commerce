from datetime import date, datetime, timezone
import os
from pyspark.sql import Window
from pyspark.sql import functions as spark_functions
from delta.tables import DeltaTable
from data_quality.dq_landing_raw import LANDING_DATASETS
from observability.obs_transformation_log import write_transformation_log
from path_constants.path_constants import BUCKET_BRO, BUCKET_RAW
from schemas.schemas import BRONZE_DATASET_SCHEMAS
from utils.logger import log


PIPELINE_NAME = "raw_to_bronze"


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _raw_prefix_exists(s3_client, prefix):
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_RAW,
        Prefix=prefix,
        MaxKeys=1,
    )
    return bool(response.get("Contents"))


def _read_raw_dataframe(spark, path, file_format):
    if file_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if file_format == "json":
        return spark.read.option("multiLine", True).json(path)
    if file_format == "parquet":
        return spark.read.parquet(path)
    raise ValueError(f"Unsupported file format: {file_format}")


def _normalized_value(column_name):
    value_as_text = spark_functions.trim(
        spark_functions.col(column_name).cast("string")
    )
    return spark_functions.when(
        spark_functions.lower(value_as_text).isin("", "null", "n/a"),
        spark_functions.lit(None),
    ).otherwise(value_as_text)


def _apply_bronze_schema(dataframe, schema):
    missing_columns = [
        field.name for field in schema.fields if field.name not in dataframe.columns
    ]
    if missing_columns:
        raise ValueError("Missing Raw columns: " + ", ".join(sorted(missing_columns)))

    cast_failures = []
    projected_columns = []
    for field in schema.fields:
        normalized_value = _normalized_value(field.name)
        cast_value = normalized_value.cast(field.dataType)
        cast_failures.append(normalized_value.isNotNull() & cast_value.isNull())
        projected_columns.append(cast_value.alias(field.name))

    cast_failure_condition = cast_failures[0]
    for condition in cast_failures[1:]:
        cast_failure_condition = cast_failure_condition | condition

    projected_columns.append(cast_failure_condition.alias("_cast_failed"))
    projected_columns.append(spark_functions.input_file_name().alias("_source_file"))
    return dataframe.select(projected_columns)


def _add_bronze_metadata(
    dataframe,
    schema,
    config,
    run_id,
    execution_date,
    source_path,
):
    business_columns = [field.name for field in schema.fields]
    duplicate_window = Window.partitionBy(spark_functions.struct(business_columns))
    required_null_condition = spark_functions.lit(False)
    for field in config["required_fields"]:
        required_null_condition = (
            required_null_condition | spark_functions.col(field).isNull()
        )

    dataframe = dataframe.withColumn(
        "_duplicate_count",
        spark_functions.count(spark_functions.lit(1)).over(duplicate_window),
    )
    invalid_condition = (
        spark_functions.col("_cast_failed")
        | required_null_condition
        | (spark_functions.col("_duplicate_count") > 1)
    )
    hash_values = [
        spark_functions.coalesce(
            spark_functions.col(column).cast("string"),
            spark_functions.lit("<NULL>"),
        )
        for column in business_columns
    ]
    dataframe = (
        dataframe.withColumn(
            "_record_status",
            spark_functions.when(invalid_condition, "INVALID").otherwise("VALID"),
        )
        .withColumn("_bronze_run_id", spark_functions.lit(run_id))
        .withColumn("_source_name", spark_functions.lit(config["source_name"]))
        .withColumn("_source_path", spark_functions.lit(source_path))
        .withColumn("_ingestion_date", spark_functions.lit(execution_date).cast("date"))
        .withColumn("_bronze_processed_at", spark_functions.current_timestamp())
        .withColumn(
            "_record_hash",
            spark_functions.sha2(
                spark_functions.concat_ws("||", spark_functions.array(hash_values)),
                256,
            ),
        )
        .drop("_cast_failed", "_duplicate_count")
    )
    occurrence_window = Window.partitionBy("_record_hash").orderBy(
        spark_functions.col("_source_file"),
        spark_functions.monotonically_increasing_id(),
    )
    return dataframe.withColumn(
        "_record_occurrence",
        spark_functions.row_number().over(occurrence_window),
    )


def _write_new_unpartitioned_table(dataframe, target_path):
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "false")
        .option(
            "optimizeWrite",
            os.getenv("SPARK_DELTA_OPTIMIZE_WRITE", "false"),
        )
        .save(target_path)
    )


def _write_bronze_delta(spark, dataframe, target_path, execution_date):
    partition = execution_date.isoformat()
    if not DeltaTable.isDeltaTable(spark, target_path):
        _write_new_unpartitioned_table(dataframe, target_path)
        return

    delta_table = DeltaTable.forPath(spark, target_path)
    partition_columns = delta_table.detail().select("partitionColumns").first()[0]
    if partition_columns:
        log.warning(
            "Bronze table still uses legacy physical partitions: path=%s "
            "partitions=%s.",
            target_path,
            partition_columns,
        )
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_ingestion_date = DATE '{partition}'")
            .option("mergeSchema", "false")
            .option(
                "optimizeWrite",
                os.getenv("SPARK_DELTA_OPTIMIZE_WRITE", "false"),
            )
            .partitionBy("_ingestion_date")
            .save(target_path)
        )
        return

    merge_condition = (
        "target._ingestion_date = source._ingestion_date AND "
        "target._record_hash = source._record_hash AND "
        "target._record_occurrence = source._record_occurrence"
    )
    (
        delta_table.alias("target")
        .merge(dataframe.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(
            condition=f"target._ingestion_date = DATE '{partition}'"
        )
        .execute()
    )


def _transformation_event(
    run_id,
    dataset,
    source_path,
    target_path,
    records_input,
    records_output,
    records_rejected,
    data_quality_status,
    started_at,
    ended_at,
    status,
    error_message,
    execution_date,
):
    return {
        "run_id": run_id,
        "pipeline_name": PIPELINE_NAME,
        "stage": "bronze",
        "source_table": dataset,
        "target_table": dataset,
        "source_path": source_path,
        "target_path": target_path,
        "records_input": records_input,
        "records_output": records_output,
        "records_rejected": records_rejected,
        "records_inserted": records_output if status == "SUCCESS" else 0,
        "records_updated": 0,
        "records_deleted": 0,
        "data_quality_status": data_quality_status,
        "processing_start_ts": started_at,
        "processing_end_ts": ended_at,
        "duration_seconds": (ended_at - started_at).total_seconds(),
        "status": status,
        "error_message": error_message,
        "execution_date": execution_date,
    }


def transformation_raw_bronze(spark, run_id, execution_date, s3_client):
    if spark is None:
        raise ValueError(
            "A Spark session is required for Raw to Bronze transformation."
        )
    if s3_client is None:
        raise ConnectionError(
            "Could not create the S3/MinIO client for transformation."
        )
    if not isinstance(execution_date, date):
        raise TypeError("execution_date must be a date.")

    partition = execution_date.strftime("%Y%m%d")
    events = []
    for dataset, schema in BRONZE_DATASET_SCHEMAS.items():
        config = LANDING_DATASETS[dataset]
        raw_prefix = f"{dataset}/ingestion_date_{partition}/"
        if not _raw_prefix_exists(s3_client, raw_prefix):
            continue

        started_at = _utc_now()
        source_path = f"s3a://{BUCKET_RAW}/{raw_prefix}"
        target_path = f"s3a://{BUCKET_BRO}/{dataset}"
        records_input = 0
        log.info(
            "Transforming Raw to Bronze: dataset=%s source=%s target=%s.",
            dataset,
            source_path,
            target_path,
        )
        try:
            dataframe = _read_raw_dataframe(spark, source_path, config["format"])
            records_input = dataframe.count()
            dataframe = _apply_bronze_schema(dataframe, schema)
            dataframe = _add_bronze_metadata(
                dataframe,
                schema,
                config,
                run_id,
                execution_date,
                source_path.replace("s3a://", "s3://", 1),
            )
            dataframe = dataframe.cache()
            try:
                records_invalid = dataframe.filter(
                    spark_functions.col("_record_status") == "INVALID"
                ).count()
                records_output = dataframe.count()
                _write_bronze_delta(
                    spark,
                    dataframe,
                    target_path,
                    execution_date,
                )
            finally:
                dataframe.unpersist()
            ended_at = _utc_now()
            quality_status = "WARNING" if records_invalid else "PASS"
            events.append(
                _transformation_event(
                    run_id,
                    dataset,
                    source_path.replace("s3a://", "s3://", 1),
                    target_path.replace("s3a://", "s3://", 1),
                    records_input,
                    records_output,
                    0,
                    quality_status,
                    started_at,
                    ended_at,
                    "SUCCESS",
                    None,
                    execution_date,
                )
            )
            log.info(
                "Raw to Bronze finished: dataset=%s input=%s output=%s invalid=%s.",
                dataset,
                records_input,
                records_output,
                records_invalid,
            )
        except Exception as error:
            ended_at = _utc_now()
            log.exception("Raw to Bronze failed: dataset=%s.", dataset)
            events.append(
                _transformation_event(
                    run_id,
                    dataset,
                    source_path.replace("s3a://", "s3://", 1),
                    target_path.replace("s3a://", "s3://", 1),
                    records_input,
                    0,
                    records_input,
                    "FAIL",
                    started_at,
                    ended_at,
                    "FAILED",
                    f"{type(error).__name__}: {error}",
                    execution_date,
                )
            )

    write_transformation_log(spark, events)
    return events


if __name__ == "__main__":
    from utils.job import job_arguments, job_spark, required_s3_client

    arguments = job_arguments("Transform Raw data into Bronze.")
    with job_spark("raw_to_bronze") as spark_session:
        transformation_raw_bronze(
            spark_session,
            arguments.run_id,
            arguments.execution_date,
            required_s3_client(),
        )
