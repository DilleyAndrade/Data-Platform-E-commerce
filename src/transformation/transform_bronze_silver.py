from datetime import date, datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as spark_functions
from pyspark.sql.types import StringType

from observability.obs_transformation_log import write_transformation_log
from path_constants.path_constants import BUCKET_BRO, BUCKET_QUA, BUCKET_REJ, BUCKET_SIL
from schemas.schemas import (
    BRONZE_DATASET_SCHEMAS,
    DATASET_PRIMARY_KEYS,
    DATASET_REQUIRED_FIELDS,
)
from utils.logger import log


PIPELINE_NAME = "bronze_to_silver"

SILVER_DATASETS = (
    "customers",
    "suppliers",
    "products",
    "inventory",
    "coupons",
    "orders",
    "order_items",
    "delivery_tracking",
    "payments",
    "website_events",
    "customer_review",
    "exchange_rates",
    "marketing_campaigns",
)

PRIMARY_KEYS = DATASET_PRIMARY_KEYS

FOREIGN_KEYS = {
    "products": [("supplier_id", "suppliers", "supplier_id")],
    "inventory": [("product_id", "products", "product_id")],
    "orders": [
        ("customer_id", "customers", "customer_id"),
        ("coupon_code", "coupons", "coupon"),
    ],
    "order_items": [
        ("order_id", "orders", "order_id"),
        ("product_id", "products", "product_id"),
    ],
    "delivery_tracking": [("order_id", "orders", "order_id")],
    "payments": [("order_id", "orders", "order_id")],
    "website_events": [
        ("customer_id", "customers", "customer_id"),
        ("product_id", "products", "product_id"),
        ("order_id", "orders", "order_id"),
    ],
    "customer_review": [
        ("customer_id", "customers", "customer_id"),
        ("product_id", "products", "product_id"),
        ("order_id", "orders", "order_id"),
    ],
}

LOWERCASE_COLUMNS = {
    "customers": ["email", "city", "state", "customer_type", "status"],
    "suppliers": ["city", "state"],
    "products": ["category", "subcategory", "brand"],
    "orders": ["status"],
    "delivery_tracking": ["status"],
    "payments": ["payment_method", "payment_status"],
    "website_events": ["event", "page"],
    "marketing_campaigns": ["channel", "status"],
}

VALID_STATUS_VALUES = {
    "orders": {
        "pending",
        "paid",
        "shipped",
        "delivered",
        "cancelled",
        "refunded",
    },
    "delivery_tracking": {
        "created",
        "shipped",
        "in_transit",
        "out_for_delivery",
        "delivered",
        "failed",
        "returned",
    },
    "payments": {
        "pending",
        "authorized",
        "approved",
        "declined",
        "partially_refunded",
        "refunded",
        "cancelled",
    },
    "customers": {"active", "inactive", "blocked"},
}

OUTLIER_LIMITS = {
    "payments": {"amount": 1000000},
    "products": {"price": 100000},
    "inventory": {"quantity_available": 1000000},
    "order_items": {"quantity": 10000, "unit_price": 100000},
    "marketing_campaigns": {"budget": 100000000},
}

MINIMUM_COMPLETENESS = 0.80


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _delta_exists(spark, path):
    return DeltaTable.isDeltaTable(spark, path)


def _read_incremental_bronze(spark, path, run_id, execution_date):
    if not _delta_exists(spark, path):
        return None
    return (
        spark.read.format("delta")
        .load(path)
        .filter(spark_functions.col("_bronze_run_id") == run_id)
        .filter(spark_functions.col("_ingestion_date") == execution_date)
    )


def _standardize(dataframe, dataset):
    lowercase_columns = set(LOWERCASE_COLUMNS.get(dataset, []))
    for field in BRONZE_DATASET_SCHEMAS[dataset].fields:
        value = spark_functions.col(field.name)
        if isinstance(field.dataType, StringType):
            value = spark_functions.trim(value)
            if field.name in lowercase_columns:
                value = spark_functions.lower(value)
        dataframe = dataframe.withColumn(field.name, value.cast(field.dataType))
    return dataframe


def _add_calculated_fields(dataframe, dataset, execution_date):
    # Values that are now part of the source contract must be preserved. Their
    # formulas and consistency are checked below instead of being overwritten.
    return dataframe


def _append_reason(current_reason, condition, reason):
    # concat_ws ignores nulls. Referencing the previous expression only once
    # keeps generated Spark code linear as the number of rules grows.
    previous_reason = spark_functions.when(
        spark_functions.length(current_reason) > 0,
        current_reason,
    )
    new_reason = spark_functions.when(condition, spark_functions.lit(reason))
    return spark_functions.concat_ws(";", previous_reason, new_reason)


def _apply_row_quality_rules(dataframe, dataset, execution_date):
    reject_reason = spark_functions.lit("")
    quarantine_reason = spark_functions.lit("")
    for key in PRIMARY_KEYS[dataset]:
        field = BRONZE_DATASET_SCHEMAS[dataset][key]
        invalid_key = spark_functions.col(key).isNull()
        if isinstance(field.dataType, StringType):
            invalid_key = invalid_key | (spark_functions.col(key) == "")
        reject_reason = _append_reason(
            reject_reason,
            invalid_key,
            f"NULL_PRIMARY_KEY:{key}",
        )

    rules = []
    if dataset == "customer_review":
        rules.append(
            (
                spark_functions.col("rating").isNull()
                | ~spark_functions.col("rating").between(1, 5),
                "INVALID_RATING",
            )
        )
    if dataset == "coupons":
        rules.extend(
            [
                (
                    spark_functions.col("discount").isNull()
                    | (spark_functions.col("discount") < 0),
                    "INVALID_DISCOUNT",
                ),
                (
                    (spark_functions.col("discount_type") == "percentage")
                    & (spark_functions.col("discount") > 100),
                    "PERCENTAGE_DISCOUNT_ABOVE_100",
                ),
                (
                    spark_functions.col("end_date") < spark_functions.col("start_date"),
                    "INVALID_DATE_RANGE",
                ),
                (
                    ~spark_functions.col("discount_type").isin("percentage", "fixed"),
                    "INVALID_DISCOUNT_TYPE",
                ),
            ]
        )
    if dataset in ("payments", "products", "marketing_campaigns"):
        amount_column = {
            "payments": "amount",
            "products": "price",
            "marketing_campaigns": "budget",
        }[dataset]
        rules.append(
            (
                spark_functions.col(amount_column).isNull()
                | (spark_functions.col(amount_column) < 0),
                f"INVALID_{amount_column.upper()}",
            )
        )
    if dataset == "inventory":
        for quantity_column in (
            "quantity_available",
            "quantity_reserved",
            "quantity_damaged",
            "reorder_point",
            "safety_stock",
        ):
            rules.append(
                (
                    spark_functions.col(quantity_column).isNull()
                    | (spark_functions.col(quantity_column) < 0),
                    f"INVALID_{quantity_column.upper()}",
                )
            )
    if dataset == "order_items":
        rules.extend(
            [
                (
                    spark_functions.col("quantity").isNull()
                    | (spark_functions.col("quantity") <= 0),
                    "INVALID_QUANTITY",
                ),
                (
                    spark_functions.col("unit_price").isNull()
                    | (spark_functions.col("unit_price") < 0),
                    "INVALID_UNIT_PRICE",
                ),
                (
                    spark_functions.abs(
                        spark_functions.col("line_total")
                        - (
                            spark_functions.col("quantity")
                            * spark_functions.col("unit_price")
                            - spark_functions.col("discount_amount")
                            + spark_functions.col("tax_amount")
                        )
                    ) > spark_functions.lit(0.01),
                    "INVALID_LINE_TOTAL",
                ),
            ]
        )
    if dataset == "orders":
        rules.append(
            (
                spark_functions.abs(
                    spark_functions.col("total_amount")
                    - (
                        spark_functions.col("subtotal_amount")
                        - spark_functions.col("discount_amount")
                        + spark_functions.col("shipping_amount")
                        + spark_functions.col("tax_amount")
                    )
                ) > spark_functions.lit(0.01),
                "INVALID_ORDER_TOTAL",
            )
        )
    if dataset == "payments":
        rules.extend(
            [
                (spark_functions.col("refunded_amount") < 0, "NEGATIVE_REFUND"),
                (
                    spark_functions.col("refunded_amount")
                    > spark_functions.col("amount"),
                    "REFUND_ABOVE_PAYMENT_AMOUNT",
                ),
                (
                    ~spark_functions.col("payment_method").isin(
                        "pix", "credit_card", "debit_card", "boleto"
                    ),
                    "INVALID_PAYMENT_METHOD",
                ),
            ]
        )
    if dataset == "customers":
        rules.append(
            (
                ~spark_functions.col("customer_type").isin("individual", "business"),
                "INVALID_CUSTOMER_TYPE",
            )
        )
    if dataset == "marketing_campaigns":
        rules.append(
            (
                spark_functions.col("end_date") < spark_functions.col("start_date"),
                "INVALID_DATE_RANGE",
            )
        )
    if dataset == "exchange_rates":
        rules.extend(
            [
                (
                    spark_functions.col("usd_brl").isNull()
                    | (spark_functions.col("usd_brl") <= 0),
                    "INVALID_USD_BRL",
                ),
                (
                    spark_functions.col("eur_brl").isNull()
                    | (spark_functions.col("eur_brl") <= 0),
                    "INVALID_EUR_BRL",
                ),
            ]
        )

    if dataset in VALID_STATUS_VALUES:
        status_column = "payment_status" if dataset == "payments" else "status"
        rules.append(
            (
                spark_functions.col(status_column).isNull()
                | ~spark_functions.col(status_column).isin(
                    list(VALID_STATUS_VALUES[dataset])
                ),
                f"INVALID_STATUS:{status_column}",
            )
        )

    for currency_column in ("currency", "base_currency"):
        if currency_column in dataframe.columns:
            rules.append(
                (
                    ~spark_functions.col(currency_column).rlike("^[A-Z]{3}$"),
                    f"INVALID_CURRENCY:{currency_column}",
                )
            )
    for country_column in ("country_code", "shipping_country_code"):
        if country_column in dataframe.columns:
            rules.append(
                (
                    ~spark_functions.col(country_column).rlike("^[A-Z]{2}$"),
                    f"INVALID_COUNTRY_CODE:{country_column}",
                )
            )

    for condition, message in rules:
        reject_reason = _append_reason(reject_reason, condition, message)

    populated_fields = []
    required_names = set(DATASET_REQUIRED_FIELDS[dataset])
    for field in BRONZE_DATASET_SCHEMAS[dataset].fields:
        if field.name not in required_names:
            continue
        populated = spark_functions.col(field.name).isNotNull()
        if isinstance(field.dataType, StringType):
            populated = populated & (spark_functions.col(field.name) != "")
        populated_fields.append(spark_functions.when(populated, 1).otherwise(0))
    populated_count = populated_fields[0]
    for populated in populated_fields[1:]:
        populated_count = populated_count + populated
    completeness = populated_count / spark_functions.lit(len(populated_fields))
    quarantine_reason = _append_reason(
        quarantine_reason,
        completeness < MINIMUM_COMPLETENESS,
        f"LOW_COMPLETENESS:minimum={MINIMUM_COMPLETENESS}",
    )

    for column, limit in OUTLIER_LIMITS.get(dataset, {}).items():
        quarantine_reason = _append_reason(
            quarantine_reason,
            spark_functions.col(column) > limit,
            f"OUTLIER:{column}>{limit}",
        )

    end_of_expected_period = spark_functions.date_add(
        spark_functions.lit(execution_date), 1
    )
    temporal_columns = {
        "orders": "order_date",
        "delivery_tracking": "occurred_at",
        "website_events": "timestamp",
        "customers": "created_at",
        "inventory": "updated_at",
        "exchange_rates": "date",
    }
    if dataset in temporal_columns:
        temporal_column = temporal_columns[dataset]
        quarantine_reason = _append_reason(
            quarantine_reason,
            spark_functions.to_date(spark_functions.col(temporal_column))
            > end_of_expected_period,
            f"FUTURE_DATE:{temporal_column}",
        )

    if {"created_at", "updated_at"}.issubset(dataframe.columns):
        reject_reason = _append_reason(
            reject_reason,
            spark_functions.col("updated_at") < spark_functions.col("created_at"),
            "UPDATED_AT_BEFORE_CREATED_AT",
        )

    return (
        dataframe.withColumn("_reject_reason", reject_reason)
        .withColumn("_quarantine_reason", quarantine_reason)
        .withColumn("_completeness_percentage", completeness * 100)
    )


def _apply_foreign_key_rules(spark, dataframe, dataset):
    for child_column, parent_dataset, parent_column in FOREIGN_KEYS.get(dataset, []):
        parent_path = f"s3a://{BUCKET_SIL}/{parent_dataset}"
        missing_reason = f"FOREIGN_KEY_NOT_FOUND:{child_column}->{parent_dataset}"
        if not _delta_exists(spark, parent_path):
            dataframe = dataframe.withColumn(
                "_reject_reason",
                _append_reason(
                    spark_functions.col("_reject_reason"),
                    spark_functions.col(child_column).isNotNull(),
                    missing_reason,
                ),
            )
            continue

        parent_keys = (
            spark.read.format("delta")
            .load(parent_path)
            .select(spark_functions.col(parent_column).alias("__fk_value"))
            .distinct()
            .withColumn("__fk_exists", spark_functions.lit(True))
        )
        dataframe = dataframe.join(
            parent_keys,
            spark_functions.col(child_column) == spark_functions.col("__fk_value"),
            "left",
        )
        dataframe = dataframe.withColumn(
            "_reject_reason",
            _append_reason(
                spark_functions.col("_reject_reason"),
                spark_functions.col(child_column).isNotNull()
                & spark_functions.col("__fk_exists").isNull(),
                missing_reason,
            ),
        ).drop("__fk_value", "__fk_exists")
    return dataframe


def _apply_cross_table_rules(spark, dataframe, dataset):
    if dataset != "delivery_tracking":
        return dataframe

    orders_path = f"s3a://{BUCKET_SIL}/orders"
    if not _delta_exists(spark, orders_path):
        return dataframe
    order_dates = (
        spark.read.format("delta")
        .load(orders_path)
        .select(
            spark_functions.col("order_id").alias("__date_order_id"),
            spark_functions.col("order_date").alias("__order_date"),
        )
    )
    dataframe = dataframe.join(
        order_dates,
        spark_functions.col("order_id") == spark_functions.col("__date_order_id"),
        "left",
    )
    dataframe = dataframe.withColumn(
        "_reject_reason",
        _append_reason(
            spark_functions.col("_reject_reason"),
            spark_functions.col("updated_at") < spark_functions.col("__order_date"),
            "DELIVERY_BEFORE_ORDER_DATE",
        ),
    )
    return dataframe.drop("__date_order_id", "__order_date")


def _consolidate_cdc(dataframe, dataset):
    window = Window.partitionBy(PRIMARY_KEYS[dataset]).orderBy(
        spark_functions.col("updated_at").desc(),
        spark_functions.col("_bronze_processed_at").desc(),
        spark_functions.col("_record_occurrence").desc(),
    )
    return (
        dataframe.withColumn(
            "__cdc_position", spark_functions.row_number().over(window)
        )
        .filter(spark_functions.col("__cdc_position") == 1)
        .drop("__cdc_position")
    )


def _silver_columns(dataframe, dataset, run_id):
    source_metadata = ["_source_file", "_source_path", "_ingestion_date"]
    business_columns = [field.name for field in BRONZE_DATASET_SCHEMAS[dataset].fields]

    hash_columns = [
        spark_functions.coalesce(
            spark_functions.col(column).cast("string"),
            spark_functions.lit("<NULL>"),
        )
        for column in business_columns
    ]
    dataframe = (
        dataframe.withColumn(
            "_silver_record_hash",
            spark_functions.sha2(
                spark_functions.concat_ws("||", spark_functions.array(hash_columns)),
                256,
            ),
        )
        .withColumn("_silver_run_id", spark_functions.lit(run_id))
        .withColumn("_silver_processed_at", spark_functions.current_timestamp())
    )
    selected_columns = (
        business_columns
        + source_metadata
        + [
            "_silver_record_hash",
            "_silver_run_id",
            "_silver_processed_at",
        ]
    )
    return dataframe.select(selected_columns)


def _count_upsert_changes(spark, dataframe, dataset, target_path):
    total = dataframe.count()
    if not _delta_exists(spark, target_path):
        return total, 0

    target = spark.read.format("delta").load(target_path)
    join_condition = None
    for key in PRIMARY_KEYS[dataset]:
        key_match = spark_functions.col(f"source.{key}") == spark_functions.col(
            f"target.{key}"
        )
        join_condition = (
            key_match if join_condition is None else join_condition & key_match
        )

    comparison = dataframe.alias("source").join(
        target.alias("target"),
        join_condition,
        "left",
    )
    first_key = PRIMARY_KEYS[dataset][0]
    inserted = comparison.filter(
        spark_functions.col(f"target.{first_key}").isNull()
    ).count()
    updated = comparison.filter(
        spark_functions.col(f"target.{first_key}").isNotNull()
        & (
            spark_functions.col("source.updated_at")
            >= spark_functions.col("target.updated_at")
        )
        & (
            spark_functions.col("source._silver_record_hash")
            != spark_functions.col("target._silver_record_hash")
        )
    ).count()
    return inserted, updated


def _merge_silver(spark, dataframe, dataset, target_path):
    if not _delta_exists(spark, target_path):
        dataframe.write.format("delta").mode("overwrite").save(target_path)
        return

    merge_parts = [f"target.{key} = source.{key}" for key in PRIMARY_KEYS[dataset]]
    merge_condition = " AND ".join(merge_parts)
    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(dataframe.alias("source"), merge_condition)
        .whenMatchedUpdateAll(
            condition=(
                "source.updated_at >= target.updated_at AND "
                "target._silver_record_hash <> source._silver_record_hash"
            )
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_rejected(spark, dataframe, dataset, run_id, execution_date):
    target_path = f"s3a://{BUCKET_REJ}/bronze_to_silver/{dataset}"
    reject_hash_columns = [spark_functions.col(column) for column in dataframe.columns]
    rejected = (
        dataframe.withColumn(
            "_reject_record_hash",
            spark_functions.sha2(
                spark_functions.to_json(spark_functions.struct(reject_hash_columns)),
                256,
            ),
        )
        .withColumn("_reject_stage", spark_functions.lit("bronze_to_silver"))
        .withColumn("_reject_run_id", spark_functions.lit(run_id))
        .withColumn("_rejected_at", spark_functions.current_timestamp())
        .withColumn("_reject_execution_date", spark_functions.lit(execution_date))
    )
    if not _delta_exists(spark, target_path):
        rejected.write.format("delta").mode("overwrite").save(target_path)
        return

    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(
            rejected.alias("source"),
            "target._reject_run_id = source._reject_run_id AND "
            "target._reject_record_hash = source._reject_record_hash",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_quarantined(spark, dataframe, dataset, run_id, execution_date):
    target_path = f"s3a://{BUCKET_QUA}/silver/{dataset}"
    hash_columns = [spark_functions.col(column) for column in dataframe.columns]
    quarantined = (
        dataframe.withColumn(
            "_quarantine_record_hash",
            spark_functions.sha2(
                spark_functions.to_json(spark_functions.struct(hash_columns)),
                256,
            ),
        )
        .withColumn("_quarantine_stage", spark_functions.lit("silver"))
        .withColumn("_quarantine_run_id", spark_functions.lit(run_id))
        .withColumn("_quarantined_at", spark_functions.current_timestamp())
        .withColumn("_quarantine_execution_date", spark_functions.lit(execution_date))
    )
    if not _delta_exists(spark, target_path):
        quarantined.write.format("delta").mode("overwrite").save(target_path)
        return

    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(
            quarantined.alias("source"),
            "target._quarantine_run_id = source._quarantine_run_id AND "
            "target._quarantine_record_hash = source._quarantine_record_hash",
        )
        .whenNotMatchedInsertAll()
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
    records_inserted,
    records_updated,
    started_at,
    ended_at,
    status,
    error_message,
    execution_date,
):
    if status == "FAILED":
        quality_status = "FAIL"
    elif records_rejected:
        quality_status = "WARNING"
    else:
        quality_status = "PASS"
    return {
        "run_id": run_id,
        "pipeline_name": PIPELINE_NAME,
        "stage": "silver",
        "source_table": dataset,
        "target_table": dataset,
        "source_path": source_path.replace("s3a://", "s3://", 1),
        "target_path": target_path.replace("s3a://", "s3://", 1),
        "records_input": records_input,
        "records_output": records_output,
        "records_rejected": records_rejected,
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "records_deleted": 0,
        "data_quality_status": quality_status,
        "processing_start_ts": started_at,
        "processing_end_ts": ended_at,
        "duration_seconds": (ended_at - started_at).total_seconds(),
        "status": status,
        "error_message": error_message,
        "execution_date": execution_date,
    }


def transform_bronze_silver(spark, run_id, execution_date):
    if spark is None:
        raise ValueError(
            "A Spark session is required for Bronze to Silver transformation."
        )
    if not isinstance(execution_date, date):
        raise TypeError("execution_date must be a date.")

    events = []
    for dataset in SILVER_DATASETS:
        source_path = f"s3a://{BUCKET_BRO}/{dataset}"
        target_path = f"s3a://{BUCKET_SIL}/{dataset}"
        started_at = _utc_now()
        records_input = 0
        try:
            bronze = _read_incremental_bronze(
                spark,
                source_path,
                run_id,
                execution_date,
            )
            if bronze is None:
                continue
            bronze = bronze.cache()
            records_input = bronze.count()
            if records_input == 0:
                bronze.unpersist()
                continue

            log.info(
                "Transforming Bronze to Silver: dataset=%s source=%s target=%s.",
                dataset,
                source_path,
                target_path,
            )
            bronze_rejected = bronze.filter(
                spark_functions.col("_record_status") != "VALID"
            ).withColumn(
                "_reject_reason",
                spark_functions.lit("BRONZE_RECORD_INVALID"),
            )
            candidates = bronze.filter(spark_functions.col("_record_status") == "VALID")
            candidates = _standardize(candidates, dataset)
            candidates = _add_calculated_fields(candidates, dataset, execution_date)
            candidates = _apply_row_quality_rules(
                candidates,
                dataset,
                execution_date,
            )
            candidates = _apply_foreign_key_rules(spark, candidates, dataset)
            candidates = _apply_cross_table_rules(spark, candidates, dataset)

            silver_rejected = candidates.filter(
                spark_functions.col("_reject_reason") != ""
            )
            silver_quarantined = candidates.filter(
                (spark_functions.col("_reject_reason") == "")
                & (spark_functions.col("_quarantine_reason") != "")
            )
            accepted = candidates.filter(
                spark_functions.col("_reject_reason") == ""
            ).filter(spark_functions.col("_quarantine_reason") == "")
            accepted = _consolidate_cdc(accepted, dataset)
            accepted = _silver_columns(accepted, dataset, run_id).cache()

            records_output = accepted.count()
            bronze_rejected_count = bronze_rejected.count()
            silver_rejected_count = silver_rejected.count()
            records_quarantined = silver_quarantined.count()
            records_rejected = (
                bronze_rejected_count + silver_rejected_count + records_quarantined
            )
            if bronze_rejected_count + silver_rejected_count:
                rejected = bronze_rejected.unionByName(
                    silver_rejected,
                    allowMissingColumns=True,
                )
                _write_rejected(
                    spark,
                    rejected,
                    dataset,
                    run_id,
                    execution_date,
                )
            if records_quarantined:
                _write_quarantined(
                    spark,
                    silver_quarantined,
                    dataset,
                    run_id,
                    execution_date,
                )

            records_inserted, records_updated = _count_upsert_changes(
                spark,
                accepted,
                dataset,
                target_path,
            )
            if records_output:
                _merge_silver(spark, accepted, dataset, target_path)

            accepted.unpersist()
            bronze.unpersist()
            ended_at = _utc_now()
            events.append(
                _transformation_event(
                    run_id,
                    dataset,
                    source_path,
                    target_path,
                    records_input,
                    records_output,
                    records_rejected,
                    records_inserted,
                    records_updated,
                    started_at,
                    ended_at,
                    "SUCCESS",
                    None,
                    execution_date,
                )
            )
        except Exception as error:
            ended_at = _utc_now()
            log.exception("Bronze to Silver failed: dataset=%s.", dataset)
            events.append(
                _transformation_event(
                    run_id,
                    dataset,
                    source_path,
                    target_path,
                    records_input,
                    0,
                    records_input,
                    0,
                    0,
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
    from utils.job import job_arguments, job_spark, raise_for_failed_events

    arguments = job_arguments("Transform Bronze data into Silver.")
    with job_spark("bronze_to_silver") as spark_session:
        cli_events = transform_bronze_silver(
            spark_session,
            arguments.run_id,
            arguments.execution_date,
        )
    raise_for_failed_events(cli_events, "Bronze to Silver transformation")
