from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIRECTORY = "/opt/airflow/data-platform"
COMMON_ENV = {
    "PIPELINE_RUN_ID": "airflow__{{ run_id }}",
    "PIPELINE_EXECUTION_DATE": "{{ data_interval_end.strftime('%Y-%m-%d') }}",
    "PIPELINE_WATERMARK_UNTIL": "{{ data_interval_end.isoformat() }}",
}


def pipeline_task(task_id, script):
    return BashOperator(
        task_id=task_id,
        cwd=PROJECT_DIRECTORY,
        bash_command=(
            f'python {script} --run-id "$PIPELINE_RUN_ID" '
            '--date "$PIPELINE_EXECUTION_DATE"'
        ),
        env=COMMON_ENV,
        append_env=True,
    )


with DAG(
    dag_id="data_platform_monthly_pipeline",
    description="Executa mensalmente a ingestão e as camadas Bronze, Silver e Gold.",
    schedule="0 6 1 * *",
    start_date=datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
    },
    tags=["data-platform", "incremental", "monthly"],
) as dag:
    ingest_local = pipeline_task(
        "ingest_local",
        "src/ingestion/ingestion_local.py",
    )
    ingest_postgres = pipeline_task(
        "ingest_postgres",
        "src/ingestion/ingestion_postgres.py",
    )
    ingest_mysql = pipeline_task(
        "ingest_mysql",
        "src/ingestion/ingestion_mysql.py",
    )
    ingest_api = pipeline_task(
        "ingest_api",
        "src/ingestion/ingestion_api.py",
    )
    validate_landing = pipeline_task(
        "validate_landing_and_route_raw",
        "src/data_quality/dq_landing_raw.py",
    )
    correct_quarantine = pipeline_task(
        "correct_quarantined_data",
        "src/data_correction/data_correction.py",
    )
    raw_to_bronze = pipeline_task(
        "transform_raw_to_bronze",
        "src/transformation/transformation_raw_bronze.py",
    )
    bronze_to_silver = pipeline_task(
        "transform_bronze_to_silver",
        "src/transformation/transform_bronze_silver.py",
    )
    silver_to_gold = pipeline_task(
        "transform_silver_to_gold",
        "src/transformation/transform_silver_gold.py",
    )
    commit_watermarks = pipeline_task(
        "commit_incremental_watermarks",
        "src/ingestion/commit_incremental_state.py",
    )

    (
        ingest_local
        >> ingest_postgres
        >> ingest_mysql
        >> ingest_api
        >> validate_landing
        >> correct_quarantine
        >> raw_to_bronze
        >> bronze_to_silver
        >> silver_to_gold
        >> commit_watermarks
    )
