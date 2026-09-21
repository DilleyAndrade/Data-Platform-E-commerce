import os
from math import ceil
from datetime import date, datetime
from typing import Any
from dotenv import load_dotenv
from observability.obs_ingestion_log import create_ingestion_log, write_ingestion_log
from ingestion.incremental_state import (
    attach_watermark_candidate,
    configured_watermark_upper_bound,
    incremental_window,
    utc_now,
)
from path_constants.path_constants import BUCKET_LAN
from utils.logger import log

POSTGRES_SCHEMA = "public"
POSTGRES_TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "suppliers": "supplier_id",
}
DEFAULT_JDBC_PARTITIONS = 8
JDBC_RECORDS_PER_PARTITION = 1_000_000


def create_postgres_jdbc_config() -> dict[str, str]:
    load_dotenv()
    settings = {
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
        "database": os.getenv("PG_DB"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
    }
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise ValueError(
            "Missing required PostgreSQL settings: " + ", ".join(sorted(missing))
        )
    return {
        "url": (
            f"jdbc:postgresql://{settings['host']}:{settings['port']}/"
            f"{settings['database']}"
        ),
        "user": settings["user"],
        "password": settings["password"],
        "driver": "org.postgresql.Driver",
    }


def _jdbc_reader(spark, jdbc_config, dbtable):
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_config["url"])
        .option("dbtable", dbtable)
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .option("driver", jdbc_config["driver"])
        .option("fetchsize", 10_000)
    )


def _postgres_incremental_source(table_name, lower_bound, upper_bound):
    qualified_table = f"{POSTGRES_SCHEMA}.{table_name}"
    if lower_bound is None or upper_bound is None:
        return qualified_table
    lower = lower_bound.strftime("%Y-%m-%d %H:%M:%S.%f")
    upper = upper_bound.strftime("%Y-%m-%d %H:%M:%S.%f")
    return (
        f"(SELECT * FROM {qualified_table} "
        f"WHERE updated_at >= TIMESTAMPTZ '{lower}+00:00' "
        f"AND updated_at < TIMESTAMPTZ '{upper}+00:00') AS incremental_source"
    )


def read_postgres_table(
    spark,
    table_name,
    jdbc_config,
    num_partitions,
    lower_bound=None,
    upper_bound=None,
):
    if table_name not in POSTGRES_TABLES:
        raise ValueError(f"PostgreSQL table is not allowed: {table_name}")
    if num_partitions <= 0:
        raise ValueError("PostgreSQL JDBC partitions must be greater than zero.")

    partition_column = POSTGRES_TABLES[table_name]
    source = _postgres_incremental_source(table_name, lower_bound, upper_bound)
    bounds_query = (
        f"(SELECT MIN({partition_column}) AS lower_bound, "
        f"MAX({partition_column}) AS upper_bound, "
        f"COUNT(*) AS record_count FROM {source}) AS bounds"
    )
    bounds = _jdbc_reader(spark, jdbc_config, bounds_query).load().first()

    if bounds.lower_bound is None:
        return _jdbc_reader(spark, jdbc_config, source).load()

    effective_partitions = min(
        num_partitions,
        max(1, ceil(bounds.record_count / JDBC_RECORDS_PER_PARTITION)),
    )

    return (
        _jdbc_reader(spark, jdbc_config, source)
        .option("partitionColumn", partition_column)
        .option("lowerBound", bounds.lower_bound)
        .option("upperBound", bounds.upper_bound)
        .option("numPartitions", effective_partitions)
        .load()
    )


def build_landing_location(table_name, ingestion_date):
    date_partition = ingestion_date.strftime("%Y%m%d")
    directory = f"{table_name}/ingestion_date_{date_partition}"
    spark_path = f"s3a://{BUCKET_LAN}/{directory}/"
    log_path = f"s3://{BUCKET_LAN}/{directory}/"
    return spark_path, log_path


def ingestion_postgres(
    spark: Any,
    run_id: str,
    ingestion_date: date,
    num_partitions: int = DEFAULT_JDBC_PARTITIONS,
    watermark_upper_bound: datetime | None = None,
) -> list[dict[str, Any]]:
    log.info("Started PostgreSQL ingestion.")
    jdbc_config = create_postgres_jdbc_config()
    ingestion_logs = []

    for table_name in POSTGRES_TABLES:
        started_at = datetime.now()
        window = incremental_window(
            spark,
            "postgres",
            table_name,
            watermark_upper_bound or configured_watermark_upper_bound() or utc_now(),
        )
        spark_path, log_path = build_landing_location(table_name, ingestion_date)
        log.info(
            "Reading PostgreSQL table %s.%s with Spark JDBC.",
            POSTGRES_SCHEMA,
            table_name,
        )

        try:
            dataframe = read_postgres_table(
                spark,
                table_name,
                jdbc_config,
                num_partitions,
                window.lower_bound,
                window.upper_bound,
            )
            dataframe.write.mode("overwrite").parquet(spark_path)
            status = "SUCCESS"
            error_message = ""
        except Exception as error:
            status = "FAILED"
            error_message = f"{type(error).__name__}: {error}"
            log.exception("Failed to ingest PostgreSQL table %s.", table_name)

        ended_at = datetime.now()
        event = create_ingestion_log(
                run_id,
                "postgres",
                table_name,
                "table",
                table_name,
                log_path,
                started_at,
                ended_at,
                status,
                error_message,
            )
        if status == "SUCCESS":
            attach_watermark_candidate(event, window)
        ingestion_logs.append(event)

    write_ingestion_log(spark, ingestion_logs)
    log.info("Finished PostgreSQL ingestion.")
    return ingestion_logs


if __name__ == "__main__":
    from utils.job import job_arguments, job_spark, raise_for_failed_events

    arguments = job_arguments("Ingest PostgreSQL tables into Landing.")
    with job_spark("landing_ingestion_postgres") as spark_session:
        cli_events = ingestion_postgres(
            spark_session,
            arguments.run_id,
            arguments.execution_date,
        )
    raise_for_failed_events(cli_events, "PostgreSQL ingestion")
