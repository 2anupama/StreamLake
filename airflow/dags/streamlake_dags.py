"""Airflow 3 DAGs for the StreamLake local batch and analytics workflows."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from streamlake.orchestration.airflow_tasks import (
    create_orchestration_batch_id,
    daily_reconciliation,
    discover_candidates,
    failure_notification_callback,
    ingest_candidates,
    run_dbt_transformation,
    run_spark_transformations,
    retry_notification_callback,
    validate_candidates,
    verify_bronze,
    verify_data_quality,
    verify_gold,
    verify_silver,
    write_audit_report,
)

DEFAULT_ARGS = {
    "owner": "streamlake",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": failure_notification_callback,
    "on_retry_callback": retry_notification_callback,
}
COMMON_DAG_ARGS = {
    "start_date": pendulum.datetime(2025, 1, 1, tz="UTC"),
    "catchup": False,
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=8),
    "default_args": DEFAULT_ARGS,
    "on_failure_callback": failure_notification_callback,
    "tags": ["streamlake", "phase4"],
}
RUN_PARAMS = {
    "input_dir": Param("data/incoming", type="string", description="Input folder inside the project"),
    "batch_id": Param("", type="string", description="Optional historical batch ID"),
    "start_date": Param("", type="string", description="Optional backfill start date (YYYY-MM-DD)"),
    "end_date": Param("", type="string", description="Optional backfill end date (YYYY-MM-DD)"),
    "reprocess": Param(False, type="boolean", description="Include already completed input fingerprints"),
}


def _params() -> dict:
    context = get_current_context()
    params = dict(context.get("params", {}))
    dag_run = context.get("dag_run")
    if dag_run and dag_run.conf:
        params.update(dag_run.conf)
    return params


@dag(
    dag_id="streamlake_batch_ingestion",
    schedule=os.getenv("STREAMLAKE_BATCH_SCHEDULE", "0 2 * * *"),
    params=RUN_PARAMS,
    description="Incremental file ingestion through Bronze, Silver, Gold, dbt, and audit.",
    **COMMON_DAG_ARGS,
)
def batch_ingestion():
    @task(task_id="discover_incoming_files")
    def discover():
        return discover_candidates(_params())

    @task(task_id="validate_files")
    def validate(discovery):
        return validate_candidates(discovery)

    @task(task_id="generate_batch_id")
    def generate(validation):
        context = get_current_context()
        return create_orchestration_batch_id(validation, context.get("run_id"))

    @task(task_id="ingest_raw_data")
    def ingest(discovery, batch_info):
        return ingest_candidates(discovery, batch_info)

    @task(task_id="load_bronze")
    def bronze(ingestion):
        return verify_bronze(ingestion)

    @task(task_id="run_pyspark_transformations")
    def spark(bronze_result):
        return run_spark_transformations(bronze_result)

    @task(task_id="load_silver")
    def silver(spark_result):
        return verify_silver(spark_result)

    @task(task_id="run_data_quality_checks")
    def quality(silver_result):
        return verify_data_quality(silver_result)

    @task(task_id="load_gold")
    def gold(quality_result):
        return verify_gold(quality_result)

    @task(task_id="run_dbt_models")
    def dbt(gold_result):
        return run_dbt_transformation(gold_result)

    @task(task_id="generate_audit_report")
    def audit(discovery_result, batch_info, ingestion_result, quality_result, dbt_result):
        context = get_current_context()
        return write_audit_report(
            discovery_result,
            batch_info,
            ingestion_result,
            quality_result,
            dbt_result,
            run_id=context.get("run_id"),
        )

    @task(task_id="complete")
    def complete(audit_result):
        return {"status": "complete", **audit_result}

    discovered = discover()
    validated = validate(discovered)
    batch_info = generate(validated)
    ingested = ingest(discovered, batch_info)
    bronze_result = bronze(ingested)
    spark_result = spark(bronze_result)
    silver_result = silver(spark_result)
    quality_result = quality(silver_result)
    gold_result = gold(quality_result)
    dbt_result = dbt(gold_result)
    audit_result = audit(discovered, batch_info, ingested, quality_result, dbt_result)
    complete(audit_result)


@dag(
    dag_id="streamlake_bronze_to_silver",
    schedule=None,
    params=RUN_PARAMS,
    description="Transform existing Bronze inputs into Silver with Phase 3 Spark.",
    **COMMON_DAG_ARGS,
)
def bronze_to_silver():
    @task(task_id="check_bronze")
    def check():
        return verify_bronze()

    @task(task_id="run_pyspark_transformations")
    def transform(result):
        return run_spark_transformations(result)

    @task(task_id="load_silver")
    def verify(result):
        return verify_silver(result)

    verify(transform(check()))


@dag(
    dag_id="streamlake_silver_to_gold",
    schedule=None,
    description="Validate Silver and refresh Gold marts and dbt models.",
    **COMMON_DAG_ARGS,
)
def silver_to_gold():
    @task(task_id="check_silver")
    def check():
        return verify_silver()

    @task(task_id="run_data_quality_checks")
    def quality(result):
        return verify_data_quality(result)

    @task(task_id="load_gold")
    def gold(result):
        return verify_gold(result)

    @task(task_id="run_dbt_models")
    def dbt(result):
        return run_dbt_transformation(result)

    dbt(gold(quality(check())))


@dag(
    dag_id="streamlake_data_quality",
    schedule="0 3 * * *",
    description="Run Silver data quality and source-to-target reconciliation checks.",
    **COMMON_DAG_ARGS,
)
def data_quality():
    @task(task_id="load_silver")
    def silver():
        return verify_silver()

    @task(task_id="run_data_quality_checks")
    def quality(result):
        return verify_data_quality(result)

    quality(silver())


@dag(
    dag_id="streamlake_dbt_transformation",
    schedule=None,
    description="Build dbt local models and run their tests against DuckDB.",
    **COMMON_DAG_ARGS,
)
def dbt_transformation():
    @task(task_id="load_gold")
    def gold():
        return verify_gold()

    @task(task_id="run_dbt_models")
    def dbt(result):
        return run_dbt_transformation(result)

    dbt(gold())


@dag(
    dag_id="streamlake_daily_reconciliation",
    schedule="0 4 * * *",
    description="Reconcile batch history, Phase 3 quality outputs, and Gold marts.",
    **COMMON_DAG_ARGS,
)
def daily_reconciliation_dag():
    @task(task_id="reconcile_daily_outputs")
    def reconcile():
        return daily_reconciliation()

    reconcile()


streamlake_batch_ingestion = batch_ingestion()
streamlake_bronze_to_silver = bronze_to_silver()
streamlake_silver_to_gold = silver_to_gold()
streamlake_data_quality = data_quality()
streamlake_dbt_transformation = dbt_transformation()
streamlake_daily_reconciliation = daily_reconciliation_dag()
