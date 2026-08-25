"""Distributed PostgreSQL ingestion with Spark JDBC."""

import os
from datetime import date, datetime
from typing import Any

from dotenv import load_dotenv

from observability.obs_ingestion_log import create_ingestion_log, write_ingestion_log
from path_constants.path_constants import BUCKET_LAN
from utils.logger import log

POSTGRES_SCHEMA = "public"
POSTGRES_TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "suppliers": "supplier_id",
}
DEFAULT_JDBC_PARTITIONS = 8


def create_postgres_jdbc_config() -> dict[str, str]:
    """Build the PostgreSQL JDBC settings from environment variables."""
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


def read_postgres_table(spark, table_name, jdbc_config, num_partitions):
    """Read a PostgreSQL table in parallel using its numeric key bounds."""
    if table_name not in POSTGRES_TABLES:
        raise ValueError(f"PostgreSQL table is not allowed: {table_name}")
    if num_partitions <= 0:
        raise ValueError("PostgreSQL JDBC partitions must be greater than zero.")

    partition_column = POSTGRES_TABLES[table_name]
    qualified_table = f"{POSTGRES_SCHEMA}.{table_name}"
    bounds_query = (
        f"(SELECT MIN({partition_column}) AS lower_bound, "
        f"MAX({partition_column}) AS upper_bound FROM {qualified_table}) AS bounds"
    )
    bounds = _jdbc_reader(spark, jdbc_config, bounds_query).load().first()

    if bounds.lower_bound is None:
        return _jdbc_reader(spark, jdbc_config, qualified_table).load()

    return (
        _jdbc_reader(spark, jdbc_config, qualified_table)
        .option("partitionColumn", partition_column)
        .option("lowerBound", bounds.lower_bound)
        .option("upperBound", bounds.upper_bound)
        .option("numPartitions", num_partitions)
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
) -> list[dict[str, Any]]:
    """Ingest all configured PostgreSQL tables with Spark."""
    log.info("Started PostgreSQL ingestion.")
    jdbc_config = create_postgres_jdbc_config()
    ingestion_logs = []

    for table_name in POSTGRES_TABLES:
        started_at = datetime.now()
        spark_path, log_path = build_landing_location(table_name, ingestion_date)
        log.info("Reading PostgreSQL table %s.%s with Spark JDBC.", POSTGRES_SCHEMA, table_name)

        try:
            dataframe = read_postgres_table(
                spark,
                table_name,
                jdbc_config,
                num_partitions,
            )
            dataframe.write.mode("overwrite").parquet(spark_path)
            status = "SUCCESS"
            error_message = ""
        except Exception as error:
            status = "FAILED"
            error_message = f"{type(error).__name__}: {error}"
            log.exception("Failed to ingest PostgreSQL table %s.", table_name)

        ended_at = datetime.now()
        ingestion_logs.append(
            create_ingestion_log(
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
        )

    write_ingestion_log(spark, ingestion_logs)
    log.info("Finished PostgreSQL ingestion.")
    return ingestion_logs
