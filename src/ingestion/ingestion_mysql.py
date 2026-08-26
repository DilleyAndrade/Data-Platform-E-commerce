import os
from math import ceil
from datetime import date, datetime
from typing import Any
from dotenv import load_dotenv
from observability.obs_ingestion_log import create_ingestion_log, write_ingestion_log
from path_constants.path_constants import BUCKET_LAN
from utils.logger import log

MYSQL_TABLES = {
    "inventory": "product_id",
    "order_items": "order_item_id",
    "orders": "order_id",
}
DEFAULT_JDBC_PARTITIONS = 8
JDBC_RECORDS_PER_PARTITION = 1_000_000


def create_mysql_jdbc_config() -> dict[str, str]:
    load_dotenv()
    settings = {
        "host": os.getenv("MYSQL_HOST"),
        "port": os.getenv("MYSQL_PORT"),
        "database": os.getenv("MYSQL_DB"),
        "user": os.getenv("MYSQL_USER"),
        "password": os.getenv("MYSQL_PASSWORD"),
    }
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise ValueError(
            "Missing required MySQL settings: " + ", ".join(sorted(missing))
        )
    return {
        "url": (
            f"jdbc:mysql://{settings['host']}:{settings['port']}/"
            f"{settings['database']}?useCursorFetch=true&useServerPrepStmts=true"
        ),
        "user": settings["user"],
        "password": settings["password"],
        "driver": "com.mysql.cj.jdbc.Driver",
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


def read_mysql_table(spark, table_name, jdbc_config, num_partitions):
    if table_name not in MYSQL_TABLES:
        raise ValueError(f"MySQL table is not allowed: {table_name}")
    if num_partitions <= 0:
        raise ValueError("MySQL JDBC partitions must be greater than zero.")

    partition_column = MYSQL_TABLES[table_name]
    bounds_query = (
        f"(SELECT MIN({partition_column}) AS lower_bound, "
        f"MAX({partition_column}) AS upper_bound, "
        f"COUNT(*) AS record_count FROM {table_name}) AS bounds"
    )
    bounds = _jdbc_reader(spark, jdbc_config, bounds_query).load().first()

    if bounds.lower_bound is None:
        return _jdbc_reader(spark, jdbc_config, table_name).load()

    effective_partitions = min(
        num_partitions,
        max(1, ceil(bounds.record_count / JDBC_RECORDS_PER_PARTITION)),
    )

    return (
        _jdbc_reader(spark, jdbc_config, table_name)
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


def ingestion_mysql(
    spark: Any,
    run_id: str,
    ingestion_date: date,
    num_partitions: int = DEFAULT_JDBC_PARTITIONS,
) -> list[dict[str, Any]]:
    log.info("Started MySQL ingestion.")
    jdbc_config = create_mysql_jdbc_config()
    ingestion_logs = []

    for table_name in MYSQL_TABLES:
        started_at = datetime.now()
        spark_path, log_path = build_landing_location(table_name, ingestion_date)
        log.info("Reading MySQL table %s with Spark JDBC.", table_name)

        try:
            dataframe = read_mysql_table(
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
            log.exception("Failed to ingest MySQL table %s.", table_name)

        ended_at = datetime.now()
        ingestion_logs.append(
            create_ingestion_log(
                run_id,
                "mysql",
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
    log.info("Finished MySQL ingestion.")
    return ingestion_logs
