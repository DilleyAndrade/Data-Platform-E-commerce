from datetime import date, datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as spark_functions

from observability.obs_transformation_log import write_transformation_log
from path_constants.path_constants import BUCKET_GOL, BUCKET_OBS, BUCKET_SIL
from schemas.schemas import gold_validation_failure_schema
from utils.logger import log


PIPELINE_NAME = "silver_to_gold"


def _utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _silver_path(dataset):
    return f"s3a://{BUCKET_SIL}/{dataset}"


def _gold_path(datamart, table):
    return f"s3a://{BUCKET_GOL}/{datamart}/{table}"


def _read_silver(spark, dataset):
    path = _silver_path(dataset)
    if not DeltaTable.isDeltaTable(spark, path):
        raise FileNotFoundError(f"Silver table not found: {path}")
    return spark.read.format("delta").load(path)


def _with_gold_metadata(dataframe, run_id, execution_date):
    return (
        dataframe.withColumn("_gold_run_id", spark_functions.lit(run_id))
        .withColumn("_gold_execution_date", spark_functions.lit(execution_date))
        .withColumn("_gold_processed_at", spark_functions.current_timestamp())
    )


def _dimensions(silver):
    customers = silver["customers"].select(
        "customer_id",
        "name",
        "email",
        "birth_date",
        "city",
        "state",
        "created_at",
    )
    suppliers = silver["suppliers"].select(
        "supplier_id",
        "supplier_name",
        "city",
    )
    products = silver["products"].select(
        "product_id",
        "name",
        "category",
        "price",
        "supplier_id",
    )
    return {
        "dim_customers": customers,
        "dim_suppliers": suppliers,
        "dim_products": products,
    }


def _sales_detail(silver):
    orders = silver["orders"].alias("orders")
    items = silver["order_items"].alias("items")
    customers = silver["customers"].alias("customers")
    products = silver["products"].alias("products")
    suppliers = silver["suppliers"].alias("suppliers")

    sales = (
        items.join(
            orders,
            spark_functions.col("items.order_id")
            == spark_functions.col("orders.order_id"),
            "inner",
        )
        .join(
            customers,
            spark_functions.col("orders.customer_id")
            == spark_functions.col("customers.customer_id"),
            "inner",
        )
        .join(
            products,
            spark_functions.col("items.product_id")
            == spark_functions.col("products.product_id"),
            "inner",
        )
        .join(
            suppliers,
            spark_functions.col("products.supplier_id")
            == spark_functions.col("suppliers.supplier_id"),
            "inner",
        )
        .select(
            spark_functions.col("items.order_item_id"),
            spark_functions.col("orders.order_id"),
            spark_functions.col("orders.order_date"),
            spark_functions.col("orders.status").alias("order_status"),
            spark_functions.col("customers.customer_id"),
            spark_functions.col("customers.name").alias("customer_name"),
            spark_functions.col("customers.city").alias("customer_city"),
            spark_functions.col("customers.state").alias("customer_state"),
            spark_functions.col("products.product_id"),
            spark_functions.col("products.name").alias("product_name"),
            spark_functions.col("products.category"),
            spark_functions.col("suppliers.supplier_id"),
            spark_functions.col("suppliers.supplier_name"),
            spark_functions.col("items.quantity"),
            spark_functions.col("items.unit_price"),
            spark_functions.col("items.line_total").alias("gross_revenue"),
        )
    )

    daily = sales.groupBy("order_date").agg(
        spark_functions.countDistinct("order_id").alias("orders"),
        spark_functions.countDistinct("customer_id").alias("active_customers"),
        spark_functions.sum("quantity").alias("units_sold"),
        spark_functions.sum("gross_revenue").alias("gross_revenue"),
        spark_functions.avg("gross_revenue").alias("average_item_revenue"),
    )
    sales_order_totals = sales.groupBy("order_id", "order_date", "customer_id").agg(
        spark_functions.sum("quantity").alias("units"),
        spark_functions.sum("gross_revenue").alias("order_total"),
        spark_functions.count("order_item_id").alias("line_items"),
    )
    order_totals = (
        orders.select(
            spark_functions.col("orders.order_id").alias("order_id"),
            spark_functions.col("orders.order_date").alias("order_date"),
            spark_functions.col("orders.customer_id").alias("customer_id"),
        )
        .join(
            sales_order_totals.select("order_id", "units", "order_total", "line_items"),
            "order_id",
            "left",
        )
        .fillna({"units": 0, "order_total": 0, "line_items": 0})
    )
    daily = daily.join(
        sales_order_totals.groupBy("order_date").agg(
            spark_functions.avg("order_total").alias("average_order_value")
        ),
        "order_date",
        "left",
    )
    by_product = sales.groupBy(
        "product_id", "product_name", "category", "supplier_id", "supplier_name"
    ).agg(
        spark_functions.countDistinct("order_id").alias("orders"),
        spark_functions.countDistinct("customer_id").alias("customers"),
        spark_functions.sum("quantity").alias("units_sold"),
        spark_functions.sum("gross_revenue").alias("gross_revenue"),
        spark_functions.avg("unit_price").alias("average_selling_price"),
    )
    by_customer = sales.groupBy(
        "customer_id", "customer_name", "customer_city", "customer_state"
    ).agg(
        spark_functions.countDistinct("order_id").alias("orders"),
        spark_functions.sum("quantity").alias("units_purchased"),
        spark_functions.sum("gross_revenue").alias("lifetime_value"),
        spark_functions.min("order_date").alias("first_order_date"),
        spark_functions.max("order_date").alias("last_order_date"),
    )
    by_category = sales.groupBy("category").agg(
        spark_functions.countDistinct("order_id").alias("orders"),
        spark_functions.countDistinct("customer_id").alias("customers"),
        spark_functions.sum("quantity").alias("units_sold"),
        spark_functions.sum("gross_revenue").alias("gross_revenue"),
    )
    by_state = sales.groupBy("customer_state").agg(
        spark_functions.countDistinct("order_id").alias("orders"),
        spark_functions.countDistinct("customer_id").alias("customers"),
        spark_functions.sum("gross_revenue").alias("gross_revenue"),
    )
    return {
        "fact_sales": sales,
        "fact_orders": order_totals,
        "sales_daily": daily,
        "sales_by_product": by_product,
        "sales_by_customer": by_customer,
        "sales_by_category": by_category,
        "sales_by_state": by_state,
    }


def _payments(silver):
    payments = silver["payments"].alias("payments")
    orders = silver["orders"].alias("orders")
    customers = silver["customers"].alias("customers")
    fact = (
        payments.join(
            orders,
            spark_functions.col("payments.order_id")
            == spark_functions.col("orders.order_id"),
            "inner",
        )
        .join(
            customers,
            spark_functions.col("orders.customer_id")
            == spark_functions.col("customers.customer_id"),
            "inner",
        )
        .select(
            spark_functions.col("payments.payment_id"),
            spark_functions.col("payments.order_id"),
            spark_functions.col("orders.order_date"),
            spark_functions.col("customers.customer_id"),
            spark_functions.col("customers.state").alias("customer_state"),
            spark_functions.col("payments.payment_method"),
            spark_functions.col("payments.payment_status"),
            spark_functions.col("payments.amount"),
        )
    )
    summary = fact.groupBy("order_date", "payment_method", "payment_status").agg(
        spark_functions.count("payment_id").alias("payments"),
        spark_functions.countDistinct("order_id").alias("orders"),
        spark_functions.sum("amount").alias("payment_amount"),
        spark_functions.avg("amount").alias("average_payment_amount"),
    )
    return {"fact_payments": fact, "payments_daily": summary}


def _inventory(silver):
    inventory = silver["inventory"].alias("inventory")
    products = silver["products"].alias("products")
    suppliers = silver["suppliers"].alias("suppliers")
    snapshot = (
        inventory.join(
            products,
            spark_functions.col("inventory.product_id")
            == spark_functions.col("products.product_id"),
            "inner",
        )
        .join(
            suppliers,
            spark_functions.col("products.supplier_id")
            == spark_functions.col("suppliers.supplier_id"),
            "inner",
        )
        .select(
            spark_functions.col("inventory.product_id"),
            spark_functions.col("products.name").alias("product_name"),
            spark_functions.col("products.category"),
            spark_functions.col("suppliers.supplier_id"),
            spark_functions.col("suppliers.supplier_name"),
            spark_functions.col("inventory.quantity_available"),
            spark_functions.col("products.price").alias("unit_price"),
            (
                spark_functions.col("inventory.quantity_available")
                * spark_functions.col("products.price")
            ).alias("inventory_value"),
            (spark_functions.col("inventory.quantity_available") == 0).alias(
                "is_out_of_stock"
            ),
            spark_functions.col("inventory.updated_at"),
        )
    )
    summary = snapshot.groupBy("category", "supplier_id", "supplier_name").agg(
        spark_functions.count("product_id").alias("products"),
        spark_functions.sum("quantity_available").alias("units_available"),
        spark_functions.sum("inventory_value").alias("inventory_value"),
        spark_functions.sum(
            spark_functions.when(spark_functions.col("is_out_of_stock"), 1).otherwise(0)
        ).alias("out_of_stock_products"),
    )
    return {"inventory_snapshot": snapshot, "inventory_summary": summary}


def _logistics(silver):
    tracking = silver["delivery_tracking"].alias("tracking")
    orders = silver["orders"].alias("orders")
    customers = silver["customers"].alias("customers")
    fact = (
        tracking.join(
            orders,
            spark_functions.col("tracking.order_id")
            == spark_functions.col("orders.order_id"),
            "inner",
        )
        .join(
            customers,
            spark_functions.col("orders.customer_id")
            == spark_functions.col("customers.customer_id"),
            "inner",
        )
        .select(
            spark_functions.col("tracking.tracking_id"),
            spark_functions.col("tracking.order_id"),
            spark_functions.col("orders.order_date"),
            spark_functions.col("customers.customer_id"),
            spark_functions.col("customers.city").alias("customer_city"),
            spark_functions.col("customers.state").alias("customer_state"),
            spark_functions.col("tracking.status").alias("delivery_status"),
            spark_functions.col("tracking.updated_at"),
            (
                (
                    spark_functions.unix_timestamp("tracking.updated_at")
                    - spark_functions.unix_timestamp("orders.order_date")
                )
                / 3600
            ).alias("hours_since_order"),
        )
    )
    summary = fact.groupBy("delivery_status", "customer_state").agg(
        spark_functions.countDistinct("tracking_id").alias("deliveries"),
        spark_functions.countDistinct("order_id").alias("orders"),
        spark_functions.avg("hours_since_order").alias("average_hours_since_order"),
    )
    return {"fact_deliveries": fact, "delivery_performance": summary}


def _customer_experience(silver):
    reviews = silver["customer_review"].alias("reviews")
    customers = silver["customers"].alias("customers")
    products = silver["products"].alias("products")
    review_fact = (
        reviews.join(
            customers,
            spark_functions.col("reviews.customer_id")
            == spark_functions.col("customers.customer_id"),
            "inner",
        )
        .join(
            products,
            spark_functions.col("reviews.product_id")
            == spark_functions.col("products.product_id"),
            "inner",
        )
        .select(
            spark_functions.col("reviews.review_id"),
            spark_functions.col("reviews.customer_id"),
            spark_functions.col("customers.state").alias("customer_state"),
            spark_functions.col("reviews.product_id"),
            spark_functions.col("products.name").alias("product_name"),
            spark_functions.col("products.category"),
            spark_functions.col("reviews.rating"),
            spark_functions.col("reviews.comment"),
        )
    )
    review_summary = review_fact.groupBy("product_id", "product_name", "category").agg(
        spark_functions.count("review_id").alias("reviews"),
        spark_functions.avg("rating").alias("average_rating"),
        spark_functions.sum(
            spark_functions.when(spark_functions.col("rating") >= 4, 1).otherwise(0)
        ).alias("positive_reviews"),
        spark_functions.sum(
            spark_functions.when(spark_functions.col("rating") <= 2, 1).otherwise(0)
        ).alias("negative_reviews"),
    )

    events = silver["website_events"].alias("events")
    event_fact = events.join(
        customers,
        spark_functions.col("events.customer_id")
        == spark_functions.col("customers.customer_id"),
        "inner",
    ).select(
        spark_functions.col("events.event_id"),
        spark_functions.col("events.customer_id"),
        spark_functions.col("customers.state").alias("customer_state"),
        spark_functions.col("events.event"),
        spark_functions.col("events.page"),
        spark_functions.col("events.timestamp"),
        spark_functions.to_date("events.timestamp").alias("event_date"),
    )
    event_summary = event_fact.groupBy("event_date", "event", "page").agg(
        spark_functions.count("event_id").alias("events"),
        spark_functions.countDistinct("customer_id").alias("unique_customers"),
    )
    return {
        "fact_reviews": review_fact,
        "product_satisfaction": review_summary,
        "fact_website_events": event_fact,
        "digital_engagement": event_summary,
    }


def _commercial_reference(silver):
    coupons = silver["coupons"].select(
        "coupon", "discount", "start_date", "end_date", "is_active"
    )
    campaigns = silver["marketing_campaigns"].select(
        "campaign_id", "campaign", "channel", "budget"
    )
    campaign_summary = campaigns.groupBy("channel").agg(
        spark_functions.count("campaign_id").alias("campaigns"),
        spark_functions.sum("budget").alias("total_budget"),
        spark_functions.avg("budget").alias("average_budget"),
    )
    rates = silver["exchange_rates"].select("date", "usd_brl", "eur_brl")
    return {
        "coupon_portfolio": coupons,
        "campaign_portfolio": campaigns,
        "marketing_budget_by_channel": campaign_summary,
        "exchange_rates_daily": rates,
    }


def _business_360(silver):
    sales_tables = _sales_detail(silver)
    payment_tables = _payments(silver)
    inventory_tables = _inventory(silver)
    experience_tables = _customer_experience(silver)

    customer_sales = sales_tables["sales_by_customer"].select(
        "customer_id",
        "orders",
        "units_purchased",
        "lifetime_value",
        "first_order_date",
        "last_order_date",
    )
    customer_payments = (
        payment_tables["fact_payments"]
        .groupBy("customer_id")
        .agg(
            spark_functions.count("payment_id").alias("payments"),
            spark_functions.sum("amount").alias("total_paid"),
            spark_functions.sum(
                spark_functions.when(
                    spark_functions.col("payment_status").isin(
                        "approved", "paid", "completed"
                    ),
                    1,
                ).otherwise(0)
            ).alias("successful_payments"),
        )
    )
    customer_reviews = (
        experience_tables["fact_reviews"]
        .groupBy("customer_id")
        .agg(
            spark_functions.count("review_id").alias("reviews"),
            spark_functions.avg("rating").alias("average_rating"),
        )
    )
    customer_events = (
        experience_tables["fact_website_events"]
        .groupBy("customer_id")
        .agg(
            spark_functions.count("event_id").alias("website_events"),
            spark_functions.countDistinct("event_date").alias("active_web_days"),
            spark_functions.max("timestamp").alias("last_web_event_at"),
        )
    )
    customer_360 = (
        silver["customers"]
        .select(
            "customer_id", "name", "email", "birth_date", "city", "state", "created_at"
        )
        .join(customer_sales, "customer_id", "left")
        .join(customer_payments, "customer_id", "left")
        .join(customer_reviews, "customer_id", "left")
        .join(customer_events, "customer_id", "left")
        .fillna(
            {
                "orders": 0,
                "units_purchased": 0,
                "payments": 0,
                "successful_payments": 0,
                "reviews": 0,
                "website_events": 0,
                "active_web_days": 0,
            }
        )
    )

    product_sales = sales_tables["sales_by_product"].select(
        "product_id",
        "orders",
        "customers",
        "units_sold",
        "gross_revenue",
        "average_selling_price",
    )
    product_satisfaction = experience_tables["product_satisfaction"].select(
        "product_id",
        "reviews",
        "average_rating",
        "positive_reviews",
        "negative_reviews",
    )
    product_inventory = inventory_tables["inventory_snapshot"].select(
        "product_id",
        "quantity_available",
        "inventory_value",
        "is_out_of_stock",
        "updated_at",
    )
    product_360 = (
        silver["products"]
        .alias("products")
        .join(
            silver["suppliers"].alias("suppliers"),
            spark_functions.col("products.supplier_id")
            == spark_functions.col("suppliers.supplier_id"),
            "left",
        )
        .select(
            spark_functions.col("products.product_id"),
            spark_functions.col("products.name").alias("product_name"),
            spark_functions.col("products.category"),
            spark_functions.col("products.price"),
            spark_functions.col("products.supplier_id"),
            spark_functions.col("suppliers.supplier_name"),
        )
        .join(product_sales, "product_id", "left")
        .join(product_satisfaction, "product_id", "left")
        .join(product_inventory, "product_id", "left")
        .fillna(
            {
                "orders": 0,
                "customers": 0,
                "units_sold": 0,
                "reviews": 0,
                "positive_reviews": 0,
                "negative_reviews": 0,
                "quantity_available": 0,
                "is_out_of_stock": False,
            }
        )
    )

    order_kpis = silver["orders"].agg(
        spark_functions.countDistinct("order_id").alias("total_orders"),
        spark_functions.countDistinct(
            spark_functions.when(
                spark_functions.col("status").isin("cancelled", "canceled"),
                spark_functions.col("order_id"),
            )
        ).alias("cancelled_orders"),
    )
    sales_kpis = sales_tables["fact_sales"].agg(
        spark_functions.countDistinct("order_id").alias("total_sales_orders"),
        spark_functions.countDistinct("customer_id").alias("buying_customers"),
        spark_functions.sum("quantity").alias("units_sold"),
        spark_functions.sum("gross_revenue").alias("gross_revenue"),
    )
    payment_kpis = payment_tables["fact_payments"].agg(
        spark_functions.sum("amount").alias("total_payment_amount"),
        spark_functions.count("payment_id").alias("total_payments"),
    )
    inventory_kpis = inventory_tables["inventory_snapshot"].agg(
        spark_functions.sum("inventory_value").alias("inventory_value"),
        spark_functions.sum(
            spark_functions.when(spark_functions.col("is_out_of_stock"), 1).otherwise(0)
        ).alias("out_of_stock_products"),
    )
    experience_kpis = experience_tables["fact_reviews"].agg(
        spark_functions.avg("rating").alias("average_customer_rating"),
        spark_functions.count("review_id").alias("total_reviews"),
    )
    executive_kpis = (
        order_kpis.crossJoin(sales_kpis)
        .crossJoin(payment_kpis)
        .crossJoin(inventory_kpis)
        .crossJoin(experience_kpis)
    )
    return {
        "customer_360": customer_360,
        "product_360": product_360,
        "executive_kpis": executive_kpis,
    }


DATAMARTS = {
    "master_data": {
        "required": ["customers", "products", "suppliers"],
        "builder": _dimensions,
    },
    "sales": {
        "required": ["orders", "order_items", "customers", "products", "suppliers"],
        "builder": _sales_detail,
    },
    "finance": {
        "required": ["payments", "orders", "customers"],
        "builder": _payments,
    },
    "supply_chain": {
        "required": ["inventory", "products", "suppliers"],
        "builder": _inventory,
    },
    "logistics": {
        "required": ["delivery_tracking", "orders", "customers"],
        "builder": _logistics,
    },
    "customer_experience": {
        "required": ["customer_review", "website_events", "customers", "products"],
        "builder": _customer_experience,
    },
    "commercial": {
        "required": ["coupons", "marketing_campaigns", "exchange_rates"],
        "builder": _commercial_reference,
    },
    "executive_analytics": {
        "required": [
            "customers",
            "suppliers",
            "products",
            "inventory",
            "orders",
            "order_items",
            "payments",
            "customer_review",
            "website_events",
        ],
        "builder": _business_360,
    },
}

TABLE_GRAINS = {
    "dim_customers": ["customer_id"],
    "dim_suppliers": ["supplier_id"],
    "dim_products": ["product_id"],
    "fact_sales": ["order_item_id"],
    "fact_orders": ["order_id"],
    "sales_daily": ["order_date"],
    "sales_by_product": ["product_id"],
    "sales_by_customer": ["customer_id"],
    "sales_by_category": ["category"],
    "sales_by_state": ["customer_state"],
    "fact_payments": ["payment_id"],
    "payments_daily": ["order_date", "payment_method", "payment_status"],
    "inventory_snapshot": ["product_id"],
    "inventory_summary": ["category", "supplier_id"],
    "fact_deliveries": ["tracking_id"],
    "delivery_performance": ["delivery_status", "customer_state"],
    "fact_reviews": ["review_id"],
    "product_satisfaction": ["product_id"],
    "fact_website_events": ["event_id"],
    "digital_engagement": ["event_date", "event", "page"],
    "coupon_portfolio": ["coupon"],
    "campaign_portfolio": ["campaign_id"],
    "marketing_budget_by_channel": ["channel"],
    "exchange_rates_daily": ["date"],
    "customer_360": ["customer_id"],
    "product_360": ["product_id"],
}

NONNEGATIVE_KPIS = {
    "fact_sales": ["quantity", "unit_price", "gross_revenue"],
    "fact_orders": ["units", "order_total", "line_items"],
    "sales_daily": ["orders", "active_customers", "units_sold", "gross_revenue"],
    "sales_by_product": ["orders", "customers", "units_sold", "gross_revenue"],
    "sales_by_customer": ["orders", "units_purchased", "lifetime_value"],
    "sales_by_category": ["orders", "customers", "units_sold", "gross_revenue"],
    "sales_by_state": ["orders", "customers", "gross_revenue"],
    "fact_payments": ["amount"],
    "payments_daily": ["payments", "orders", "payment_amount"],
    "inventory_snapshot": ["quantity_available", "unit_price", "inventory_value"],
    "inventory_summary": ["products", "units_available", "inventory_value"],
    "campaign_portfolio": ["budget"],
    "marketing_budget_by_channel": ["campaigns", "total_budget"],
    "customer_360": ["orders", "units_purchased", "lifetime_value"],
    "product_360": ["orders", "units_sold", "gross_revenue"],
    "executive_kpis": [
        "total_orders",
        "total_sales_orders",
        "cancelled_orders",
        "units_sold",
        "gross_revenue",
    ],
}

SOURCE_VOLUME_CHECKS = {
    "dim_customers": "customers",
    "dim_suppliers": "suppliers",
    "dim_products": "products",
    "fact_sales": "order_items",
    "fact_orders": "orders",
    "fact_payments": "payments",
    "inventory_snapshot": "inventory",
    "fact_deliveries": "delivery_tracking",
    "fact_reviews": "customer_review",
    "fact_website_events": "website_events",
    "coupon_portfolio": "coupons",
    "campaign_portfolio": "marketing_campaigns",
    "exchange_rates_daily": "exchange_rates",
    "customer_360": "customers",
    "product_360": "products",
}

ANOMALY_METRICS = {
    "sales_daily": "gross_revenue",
    "payments_daily": "payment_amount",
    "inventory_summary": "inventory_value",
    "executive_kpis": "gross_revenue",
}


def _sum_value(dataframe, column):
    row = dataframe.agg(spark_functions.sum(column).alias("value")).first()
    return row["value"] or 0


def _validate_structure(table, dataframe):
    errors = []
    grain = TABLE_GRAINS.get(table)
    if grain:
        null_condition = None
        for column in grain:
            is_null = spark_functions.col(column).isNull()
            null_condition = (
                is_null if null_condition is None else null_condition | is_null
            )
        if dataframe.filter(null_condition).limit(1).count():
            errors.append(f"NULL_GRAIN:{table}:{','.join(grain)}")
        duplicate_count = (
            dataframe.groupBy(grain)
            .count()
            .filter(spark_functions.col("count") > 1)
            .limit(1)
            .count()
        )
        if duplicate_count:
            errors.append(f"DUPLICATE_GRAIN:{table}:{','.join(grain)}")

    negative_condition = None
    for column in NONNEGATIVE_KPIS.get(table, []):
        is_negative = spark_functions.col(column) < 0
        negative_condition = (
            is_negative
            if negative_condition is None
            else negative_condition | is_negative
        )
    if (
        negative_condition is not None
        and dataframe.filter(negative_condition).limit(1).count()
    ):
        errors.append(f"NEGATIVE_KPI:{table}")
    return errors


def _validate_volume(silver, tables):
    errors = []
    for table, source in SOURCE_VOLUME_CHECKS.items():
        if table not in tables or source not in silver:
            continue
        gold_count = tables[table].count()
        silver_count = silver[source].count()
        if gold_count != silver_count:
            errors.append(
                f"VOLUME_MISMATCH:{table}={gold_count}:{source}={silver_count}"
            )
    return errors


def _validate_sales_reconciliation(silver, tables):
    if "fact_sales" not in tables:
        return []
    errors = []
    silver_revenue = _sum_value(silver["order_items"], "line_total")
    fact_revenue = _sum_value(tables["fact_sales"], "gross_revenue")
    if silver_revenue != fact_revenue:
        errors.append(
            f"REVENUE_RECONCILIATION:silver={silver_revenue}:gold={fact_revenue}"
        )

    for table in (
        "sales_daily",
        "sales_by_product",
        "sales_by_customer",
        "sales_by_category",
        "sales_by_state",
    ):
        if table not in tables:
            continue
        metric = "lifetime_value" if table == "sales_by_customer" else "gross_revenue"
        aggregated_revenue = _sum_value(tables[table], metric)
        if aggregated_revenue != fact_revenue:
            errors.append(
                f"AGGREGATION_MISMATCH:{table}.{metric}={aggregated_revenue}:"
                f"fact_sales={fact_revenue}"
            )

    expected_periods = silver["orders"].select("order_date").distinct()
    actual_periods = tables["sales_daily"].select("order_date").distinct()
    if expected_periods.subtract(actual_periods).limit(1).count():
        errors.append("MISSING_PERIOD:sales_daily.order_date")
    return errors


def _validate_finance_reconciliation(silver, tables):
    if "fact_payments" not in tables:
        return []
    errors = []
    silver_amount = _sum_value(silver["payments"], "amount")
    fact_amount = _sum_value(tables["fact_payments"], "amount")
    daily_amount = _sum_value(tables["payments_daily"], "payment_amount")
    if silver_amount != fact_amount:
        errors.append(
            f"PAYMENT_RECONCILIATION:silver={silver_amount}:gold={fact_amount}"
        )
    if fact_amount != daily_amount:
        errors.append(f"PAYMENT_AGGREGATION:fact={fact_amount}:daily={daily_amount}")
    return errors


def _validate_kpi_thresholds(silver, tables):
    errors = []
    if "product_satisfaction" in tables:
        invalid_rating = tables["product_satisfaction"].filter(
            (spark_functions.col("average_rating") < 1)
            | (spark_functions.col("average_rating") > 5)
        )
        if invalid_rating.limit(1).count():
            errors.append("KPI_THRESHOLD:average_rating_outside_1_5")
    if "inventory_summary" in tables:
        invalid_stock = tables["inventory_summary"].filter(
            spark_functions.col("out_of_stock_products")
            > spark_functions.col("products")
        )
        if invalid_stock.limit(1).count():
            errors.append("KPI_THRESHOLD:out_of_stock_above_products")
    if "executive_kpis" in tables:
        kpis = tables["executive_kpis"].first()
        expected_orders = silver["orders"].select("order_id").distinct().count()
        expected_sales_orders = (
            silver["order_items"].select("order_id").distinct().count()
        )
        expected_cancelled_orders = (
            silver["orders"]
            .filter(spark_functions.col("status").isin("cancelled", "canceled"))
            .select("order_id")
            .distinct()
            .count()
        )
        expected_revenue = _sum_value(silver["order_items"], "line_total")
        if kpis["total_orders"] != expected_orders:
            errors.append(
                f"KPI_CORRECTNESS:total_orders={kpis['total_orders']}:"
                f"silver_orders={expected_orders}"
            )
        if kpis["total_sales_orders"] != expected_sales_orders:
            errors.append(
                f"KPI_CORRECTNESS:total_sales_orders={kpis['total_sales_orders']}:"
                f"fact_sales_orders={expected_sales_orders}"
            )
        if kpis["cancelled_orders"] != expected_cancelled_orders:
            errors.append(
                f"KPI_CORRECTNESS:cancelled_orders={kpis['cancelled_orders']}:"
                f"silver_cancelled_orders={expected_cancelled_orders}"
            )
        if (kpis["gross_revenue"] or 0) != expected_revenue:
            errors.append(
                f"KPI_CORRECTNESS:gross_revenue={kpis['gross_revenue']}:"
                f"silver_revenue={expected_revenue}"
            )
    return errors


def _validate_datamart(silver, tables):
    errors = []
    for table, dataframe in tables.items():
        errors.extend(_validate_structure(table, dataframe))
    errors.extend(_validate_volume(silver, tables))
    errors.extend(_validate_sales_reconciliation(silver, tables))
    errors.extend(_validate_finance_reconciliation(silver, tables))
    errors.extend(_validate_kpi_thresholds(silver, tables))
    return errors


def _gold_validation_failure_events(
    run_id,
    datamart,
    tables,
    validation_errors,
    execution_date,
):
    execution_ts = _utc_now()
    candidate_tables = ",".join(sorted(tables))
    return [
        {
            "run_id": run_id,
            "datamart": datamart,
            "validation_type": error.split(":", 1)[0],
            "validation_error": error,
            "candidate_tables": candidate_tables,
            "validation_status": "FAIL",
            "execution_ts": execution_ts,
            "execution_date": execution_date,
        }
        for error in validation_errors
    ]


def _write_gold_validation_failures(spark, events):
    if not events:
        return None
    target_path = f"s3a://{BUCKET_OBS}/gold_validation_failures"
    log.info(
        "Writing Gold validation failures: events=%s path=%s.",
        len(events),
        target_path,
    )
    dataframe = spark.createDataFrame(
        events,
        schema=gold_validation_failure_schema,
    )
    dataframe.write.format("delta").mode("append").save(target_path)
    log.info(
        "Gold validation failures written: events=%s path=%s.",
        len(events),
        target_path,
    )
    return dataframe


def _detect_anomalies(spark, datamart, tables):
    warnings = {}
    for table, metric in ANOMALY_METRICS.items():
        if table not in tables:
            continue
        target_path = _gold_path(datamart, table)
        if not DeltaTable.isDeltaTable(spark, target_path):
            continue
        previous = spark.read.format("delta").load(target_path)
        previous_value = _sum_value(previous, metric)
        current_value = _sum_value(tables[table], metric)
        if previous_value == 0:
            continue
        variation = abs(float(current_value - previous_value)) / abs(
            float(previous_value)
        )
        if variation > 0.50:
            warnings[table] = (
                f"ANOMALY:{metric}:variation={variation:.2%}:"
                f"previous={previous_value}:current={current_value}"
            )
    return warnings


def _write_gold_table(dataframe, target_path, run_id, execution_date):
    output = _with_gold_metadata(dataframe, run_id, execution_date).cache()
    records = output.count()
    (
        output.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )
    published = spark_functions.col("_gold_run_id") == run_id
    published_records = (
        output.sparkSession.read.format("delta")
        .load(target_path)
        .filter(published)
        .count()
    )
    if published_records != records:
        output.unpersist()
        raise ValueError(
            f"GOLD_FRESHNESS_VOLUME_MISMATCH:expected={records}:"
            f"published={published_records}"
        )
    output.unpersist()
    return records


def _transformation_event(
    run_id,
    datamart,
    table,
    source_datasets,
    records_input,
    records_output,
    started_at,
    ended_at,
    status,
    error_message,
    execution_date,
):
    target_path = _gold_path(datamart, table)
    return {
        "run_id": run_id,
        "pipeline_name": PIPELINE_NAME,
        "stage": "gold",
        "source_table": ",".join(source_datasets),
        "target_table": f"{datamart}.{table}",
        "source_path": ",".join(
            _silver_path(dataset).replace("s3a://", "s3://", 1)
            for dataset in source_datasets
        ),
        "target_path": target_path.replace("s3a://", "s3://", 1),
        "records_input": records_input,
        "records_output": records_output,
        "records_rejected": 0,
        "records_inserted": records_output if status == "SUCCESS" else 0,
        "records_updated": 0,
        "records_deleted": 0,
        "data_quality_status": (
            "FAIL" if status == "FAILED" else "WARNING" if error_message else "PASS"
        ),
        "processing_start_ts": started_at,
        "processing_end_ts": ended_at,
        "duration_seconds": (ended_at - started_at).total_seconds(),
        "status": status,
        "error_message": error_message,
        "execution_date": execution_date,
    }


def transform_silver_gold(spark, run_id, execution_date):
    if spark is None:
        raise ValueError(
            "A Spark session is required for Silver to Gold transformation."
        )
    if not isinstance(execution_date, date):
        raise TypeError("execution_date must be a date.")

    events = []
    silver_cache = {}
    silver_counts = {}
    try:
        for datamart, config in DATAMARTS.items():
            started_at = _utc_now()
            required = config["required"]
            silver = {}
            tables = {}
            records_input = 0
            try:
                for dataset in required:
                    if dataset not in silver_cache:
                        silver_cache[dataset] = _read_silver(spark, dataset).cache()
                        silver_counts[dataset] = silver_cache[dataset].count()
                    silver[dataset] = silver_cache[dataset]
                    records_input += silver_counts[dataset]

                built_tables = config["builder"](silver)
                tables = {
                    table: dataframe.cache()
                    for table, dataframe in built_tables.items()
                }
                validation_errors = _validate_datamart(silver, tables)
                if validation_errors:
                    _write_gold_validation_failures(
                        spark,
                        _gold_validation_failure_events(
                            run_id,
                            datamart,
                            tables,
                            validation_errors,
                            execution_date,
                        ),
                    )
                    raise ValueError(" | ".join(validation_errors))
                anomaly_warnings = _detect_anomalies(spark, datamart, tables)
                for table, dataframe in tables.items():
                    table_started_at = _utc_now()
                    target_path = _gold_path(datamart, table)
                    log.info(
                        "Building Gold table: datamart=%s table=%s target=%s.",
                        datamart,
                        table,
                        target_path,
                    )
                    records_output = _write_gold_table(
                        dataframe,
                        target_path,
                        run_id,
                        execution_date,
                    )
                    ended_at = _utc_now()
                    events.append(
                        _transformation_event(
                            run_id,
                            datamart,
                            table,
                            required,
                            records_input,
                            records_output,
                            table_started_at,
                            ended_at,
                            "SUCCESS",
                            anomaly_warnings.get(table),
                            execution_date,
                        )
                    )
            except FileNotFoundError as error:
                log.warning("Skipping Gold datamart=%s: %s", datamart, error)
            except Exception as error:
                ended_at = _utc_now()
                log.exception("Silver to Gold failed: datamart=%s.", datamart)
                events.append(
                    _transformation_event(
                        run_id,
                        datamart,
                        "datamart_build",
                        required,
                        records_input,
                        0,
                        started_at,
                        ended_at,
                        "FAILED",
                        f"{type(error).__name__}: {error}",
                        execution_date,
                    )
                )
            finally:
                for dataframe in tables.values():
                    dataframe.unpersist()
    finally:
        for dataframe in silver_cache.values():
            dataframe.unpersist()

    write_transformation_log(spark, events)
    return events


if __name__ == "__main__":
    from utils.job import job_arguments, job_spark

    arguments = job_arguments("Transform Silver data into Gold.")
    with job_spark("silver_to_gold") as spark_session:
        transform_silver_gold(
            spark_session,
            arguments.run_id,
            arguments.execution_date,
        )
