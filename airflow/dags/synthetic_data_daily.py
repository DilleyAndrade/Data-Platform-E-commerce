from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIRECTORY = "/opt/airflow/data-platform"
GENERATOR_ENV = {
    "GENERATOR_SEED": "{{ data_interval_end.strftime('%Y%m%d') }}",
}


def generator_task(task_id, script, average_option="--average-records", datasets=()):
    dataset_arguments = f" --datasets {' '.join(datasets)}" if datasets else ""
    return BashOperator(
        task_id=task_id,
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            f"python data_generator/{script} "
            f'{average_option} "$GENERATOR_AVERAGE_RECORDS" --seed "$GENERATOR_SEED"'
            f"{dataset_arguments}"
        ),
        env=GENERATOR_ENV,
        append_env=True,
    )


with DAG(
    dag_id="synthetic_data_daily_generation",
    description="Gera diariamente dados sintéticos nas fontes da plataforma.",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
    },
    tags=["data-platform", "synthetic-data", "daily"],
) as dag:
    generate_postgres_catalog = generator_task(
        "generate_postgres_catalog",
        "postgres_generator.py",
    )
    generate_coupons = generator_task(
        "generate_coupons",
        "local_generator.py",
        datasets=("coupons",),
    )
    generate_marketing_and_rates = generator_task(
        "generate_marketing_campaigns_and_exchange_rates",
        "api_generator.py",
        datasets=("marketing_campaigns", "exchange_rates"),
    )
    generate_mysql_commerce = generator_task(
        "generate_mysql_orders_items_and_inventory",
        "mysql_generator.py",
        average_option="--average-orders",
    )
    generate_payments_and_delivery = generator_task(
        "generate_payments_and_delivery_tracking",
        "local_generator.py",
        datasets=("payments", "delivery_tracking"),
    )
    generate_reviews = generator_task(
        "generate_customer_reviews",
        "api_generator.py",
        datasets=("customer_reviews",),
    )
    generate_website_events = generator_task(
        "generate_website_events",
        "local_generator.py",
        datasets=("website_events",),
    )

    (
        generate_postgres_catalog
        >> generate_coupons
        >> generate_marketing_and_rates
        >> generate_mysql_commerce
        >> generate_payments_and_delivery
        >> generate_reviews
        >> generate_website_events
    )
