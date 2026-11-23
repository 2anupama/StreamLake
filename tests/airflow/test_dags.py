from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow.models")
from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    project_root = Path(__file__).resolve().parents[2]
    return DagBag(dag_folder=str(project_root / "airflow" / "dags"), safe_mode=False)


def test_all_phase4_dags_import(dagbag):
    assert dagbag.import_errors == {}
    assert {
        "streamlake_batch_ingestion",
        "streamlake_bronze_to_silver",
        "streamlake_silver_to_gold",
        "streamlake_data_quality",
        "streamlake_dbt_transformation",
        "streamlake_daily_reconciliation",
    }.issubset(dagbag.dags)


def test_main_pipeline_task_chain_and_retries(dagbag):
    dag = dagbag.get_dag("streamlake_batch_ingestion")
    chain = [
        "discover_incoming_files",
        "validate_files",
        "generate_batch_id",
        "ingest_raw_data",
        "load_bronze",
        "run_pyspark_transformations",
        "load_silver",
        "run_data_quality_checks",
        "load_gold",
        "run_dbt_models",
        "generate_audit_report",
        "complete",
    ]
    for upstream, downstream in zip(chain, chain[1:]):
        assert upstream in dag.get_task(downstream).upstream_task_ids
    for task_id in chain:
        task = dag.get_task(task_id)
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=1)
        assert task.execution_timeout == timedelta(hours=2)
        assert task.on_failure_callback is not None
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_all_dags_disable_catchup_and_have_failure_callbacks(dagbag):
    for dag in dagbag.dags.values():
        assert dag.catchup is False
        assert dag.on_failure_callback is not None
        assert dag.max_active_runs == 1
