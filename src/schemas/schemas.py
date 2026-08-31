from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType,
    LongType,
    DoubleType,
    IntegerType,
    DecimalType,
)

ingestion_log_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("source_name", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("ended_at", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("execution_status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("execution_date", DateType(), True),
    ]
)

landing_quality_log_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("file_name", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("file_path", StringType(), True),
        StructField("check_name", StringType(), True),
        StructField("check_type", StringType(), True),
        StructField("records_total", LongType(), True),
        StructField("records_valid", LongType(), True),
        StructField("records_invalid", LongType(), True),
        StructField("invalid_percentage", DoubleType(), True),
        StructField("check_status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("execution_ts", TimestampType(), True),
    ]
)

data_correction_log_schema = StructType(
    [
        StructField("correction_run_id", StringType(), False),
        StructField("original_run_id", StringType(), True),
        StructField("dataset_name", StringType(), True),
        StructField("source_name", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField("original_path", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("correction_type", StringType(), True),
        StructField("records_input", LongType(), True),
        StructField("records_corrected", LongType(), True),
        StructField("records_still_invalid", LongType(), True),
        StructField("records_discarded", LongType(), True),
        StructField("correction_attempt", IntegerType(), True),
        StructField("correction_status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("correction_start_ts", TimestampType(), True),
        StructField("correction_end_ts", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("execution_date", DateType(), True),
    ]
)

transformation_log_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("pipeline_name", StringType(), True),
        StructField("stage", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("records_input", LongType(), True),
        StructField("records_output", LongType(), True),
        StructField("records_rejected", LongType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("records_updated", LongType(), True),
        StructField("records_deleted", LongType(), True),
        StructField("data_quality_status", StringType(), True),
        StructField("processing_start_ts", TimestampType(), True),
        StructField("processing_end_ts", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("execution_date", DateType(), True),
    ]
)

gold_validation_failure_schema = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("datamart", StringType(), False),
        StructField("validation_type", StringType(), False),
        StructField("validation_error", StringType(), False),
        StructField("candidate_tables", StringType(), True),
        StructField("validation_status", StringType(), False),
        StructField("execution_ts", TimestampType(), False),
        StructField("execution_date", DateType(), False),
    ]
)

BRONZE_DATASET_SCHEMAS = {
    "coupons": StructType(
        [
            StructField("coupon", StringType(), True),
            StructField("discount", DecimalType(10, 2), True),
            StructField("start_date", DateType(), True),
            StructField("end_date", DateType(), True),
        ]
    ),
    "delivery_tracking": StructType(
        [
            StructField("tracking_id", LongType(), True),
            StructField("order_id", LongType(), True),
            StructField("status", StringType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    ),
    "payments": StructType(
        [
            StructField("payment_id", LongType(), True),
            StructField("order_id", LongType(), True),
            StructField("payment_method", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("amount", DecimalType(18, 2), True),
        ]
    ),
    "website_events": StructType(
        [
            StructField("event_id", LongType(), True),
            StructField("customer_id", LongType(), True),
            StructField("event", StringType(), True),
            StructField("page", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    ),
    "customer_review": StructType(
        [
            StructField("review_id", LongType(), True),
            StructField("customer_id", LongType(), True),
            StructField("product_id", LongType(), True),
            StructField("rating", IntegerType(), True),
            StructField("comment", StringType(), True),
        ]
    ),
    "exchange_rates": StructType(
        [
            StructField("date", DateType(), True),
            StructField("usd_brl", DecimalType(12, 6), True),
            StructField("eur_brl", DecimalType(12, 6), True),
        ]
    ),
    "marketing_campaigns": StructType(
        [
            StructField("campaign_id", LongType(), True),
            StructField("campaign", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("budget", DecimalType(18, 2), True),
        ]
    ),
    "customers": StructType(
        [
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("birth_date", DateType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("created_at", DateType(), True),
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DecimalType(10, 2), True),
            StructField("supplier_id", IntegerType(), True),
        ]
    ),
    "suppliers": StructType(
        [
            StructField("supplier_id", IntegerType(), True),
            StructField("supplier_name", StringType(), True),
            StructField("city", StringType(), True),
        ]
    ),
    "inventory": StructType(
        [
            StructField("product_id", IntegerType(), True),
            StructField("quantity_available", IntegerType(), True),
            StructField("updated_at", DateType(), True),
        ]
    ),
    "order_items": StructType(
        [
            StructField("order_item_id", IntegerType(), True),
            StructField("order_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
        ]
    ),
    "orders": StructType(
        [
            StructField("order_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("order_date", DateType(), True),
            StructField("status", StringType(), True),
        ]
    ),
}
