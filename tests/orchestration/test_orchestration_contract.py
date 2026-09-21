import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_airflow_dag_contains_pipeline_tasks_in_expected_order():
    text = (ROOT / "airflow/dags/data_platform_pipeline.py").read_text(encoding="utf-8")
    expected = [
        "ingest_local", "ingest_postgres", "ingest_mysql", "ingest_api",
        "validate_landing", "correct_quarantine", "raw_to_bronze",
        "bronze_to_silver", "silver_to_gold", "commit_watermarks",
    ]
    positions = [text.index(name + " =") for name in expected]
    assert positions == sorted(positions)
    assert 'schedule="0 6 1 * *"' in text
    assert "max_active_runs=1" in text


def test_all_pipeline_always_publishes_metrics_in_finally():
    source = (ROOT / "src/run_all_pipeline.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    run_pipeline = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run_pipeline")
    try_node = next(node for node in run_pipeline.body if isinstance(node, ast.Try))
    calls = [node for statement in try_node.finalbody for node in ast.walk(statement) if isinstance(node, ast.Call)]
    assert any(isinstance(call.func, ast.Name) and call.func.id == "push_pipeline_metrics" for call in calls)

