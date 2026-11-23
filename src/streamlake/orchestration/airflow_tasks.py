"""Idempotent Airflow task implementations that reuse StreamLake pipelines."""

from __future__ import annotations

import importlib
import json
import logging
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb

from streamlake.common.settings import Settings
from streamlake.ingestion.batch.pipeline import BatchPipeline
from streamlake.ingestion.batch.schemas import SourceType, infer_source_type
from streamlake.monitoring.alerts import send_alert
from streamlake.monitoring.reliability import (
    ensure_reliability_tables,
    record_batch_audit,
    record_lineage,
    record_metric,
)

logger = logging.getLogger("streamlake.airflow")
PROJECT_ROOT = Path(os.environ.get("STREAMLAKE_ROOT", Path(__file__).resolve().parents[3])).resolve()
SOURCE_ORDER = {
    SourceType.ADDITIONS: 0,
    SourceType.UPDATES: 1,
    SourceType.DEPENDENTS: 2,
    SourceType.DELETIONS: 3,
}
SUPPORTED_SUFFIXES = {".csv", ".xlsx", ".xlsm", ".xls"}


def _settings() -> Settings:
    return Settings(
        data_root=PROJECT_ROOT / "data",
        duckdb_path=PROJECT_ROOT / "data" / "streamlake.duckdb",
        ingestion_audit_path=PROJECT_ROOT / "logs" / "ingestion_audit.jsonl",
    )


def _pipeline() -> BatchPipeline:
    return BatchPipeline(_settings())


def filter_unprocessed_candidates(
    candidates: Iterable[dict[str, str]],
    completed_fingerprints: set[str],
    *,
    reprocess: bool = False,
) -> list[dict[str, str]]:
    """Drop previously completed source fingerprints unless the run is a backfill."""
    if reprocess:
        return list(candidates)
    return [item for item in candidates if item["fingerprint"] not in completed_fingerprints]


def _resolve_input_dir(value: str | None) -> Path:
    raw = Path(value or "data/incoming")
    resolved = (PROJECT_ROOT / raw).resolve() if not raw.is_absolute() else raw.resolve()
    try:
        resolved.relative_to(PROJECT_ROOT)
    except ValueError as error:
        raise ValueError("input_dir must be inside the mounted StreamLake project") from error
    if not resolved.is_dir():
        raise FileNotFoundError(f"Input directory does not exist: {resolved}")
    return resolved


def _historical_source_names(
    pipeline: BatchPipeline,
    *,
    batch_id: str | None,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, set[str]] | None:
    if batch_id:
        connection = pipeline._connect()
        try:
            rows = connection.execute(
                "SELECT source_file, file_fingerprint FROM batch_history WHERE batch_id = ? OR duplicate_of = ?",
                [batch_id, batch_id],
            ).fetchall()
        finally:
            connection.close()
        if not rows:
            raise ValueError(f"Unknown historical batch_id: {batch_id}")
        selected: dict[str, set[str]] = {}
        for source_file, fingerprint in rows:
            selected.setdefault(source_file, set()).add(fingerprint)
        return selected
    if not start_date and not end_date:
        return None

    start = date.fromisoformat(start_date) if start_date else date.min
    end = date.fromisoformat(end_date) if end_date else date.max
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    connection = pipeline._connect()
    try:
        rows = connection.execute(
            """SELECT DISTINCT source_file, file_fingerprint FROM batch_history
               WHERE try_cast(started_at AS TIMESTAMP)::DATE >= ?
                 AND try_cast(started_at AS TIMESTAMP)::DATE <= ?
                 AND status IN ('completed', 'empty', 'skipped_duplicate')""",
            [start.isoformat(), end.isoformat()],
        ).fetchall()
    finally:
        connection.close()
    selected = {}
    for source_file, fingerprint in rows:
        selected.setdefault(source_file, set()).add(fingerprint)
    return selected


def discover_candidates(params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Find changed inputs, or the source files selected by explicit backfill parameters."""
    params = params or {}
    pipeline = _pipeline()
    input_dir = _resolve_input_dir(params.get("input_dir"))
    batch_id = str(params.get("batch_id") or "").strip() or None
    start_date = str(params.get("start_date") or "").strip() or None
    end_date = str(params.get("end_date") or "").strip() or None
    requested_names = _historical_source_names(
        pipeline, batch_id=batch_id, start_date=start_date, end_date=end_date
    )
    historical_request = bool(batch_id or start_date or end_date)
    explicit_reprocess = bool(params.get("reprocess")) or historical_request

    candidate_paths = sorted(
        (path for path in input_dir.rglob("*") if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES),
        key=lambda path: str(path.relative_to(input_dir)).lower(),
    )
    if requested_names is not None:
        name_set = set(requested_names)
        candidate_paths = [path for path in candidate_paths if path.name in name_set]
        missing = name_set - {path.name for path in candidate_paths}
        if missing:
            raise FileNotFoundError(
                "Historical source file(s) are not present under input_dir: " + ", ".join(sorted(missing))
            )

    candidates: list[dict[str, str]] = []
    for path in candidate_paths:
        try:
            source_type = infer_source_type(path)
        except ValueError:
            logger.info("Ignoring unrecognized source file %s", path.name)
            continue
        fingerprint = pipeline._fingerprint(path, source_type, pipeline.settings.source_system)
        expected_fingerprints = (requested_names or {}).get(path.name)
        if historical_request and expected_fingerprints and fingerprint not in expected_fingerprints:
            raise ValueError(
                f"Historical source {path.name!r} has changed since the selected batch; restore the archived file"
            )
        candidates.append(
            {
                "relative_path": str(path.relative_to(input_dir)),
                "source_file": path.name,
                "source_type": source_type.value,
                "fingerprint": fingerprint,
            }
        )

    connection = pipeline._connect()
    try:
        completed = {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT file_fingerprint FROM batch_history WHERE status IN ('completed', 'empty')"
            ).fetchall()
        }
    finally:
        connection.close()
    selected = filter_unprocessed_candidates(candidates, completed, reprocess=explicit_reprocess)
    logger.info(
        "Discovered %d candidate(s), selected %d; explicit_backfill=%s",
        len(candidates), len(selected), explicit_reprocess,
    )
    return {
        "input_dir": str(input_dir),
        "candidates": selected,
        "candidate_count": len(selected),
        "backfill": explicit_reprocess,
        "requested_batch_id": batch_id,
        "requested_start_date": start_date,
        "requested_end_date": end_date,
    }


def validate_candidates(discovery: dict[str, Any]) -> dict[str, Any]:
    """Validate selected files in dependency order without ingesting them."""
    candidates = discovery["candidates"]
    if not candidates:
        return {"validated_files": 0, "outcomes": [], "rows_rejected": 0}

    with tempfile.TemporaryDirectory(prefix="streamlake-airflow-validate-") as temp_dir:
        validation_dir = Path(temp_dir)
        for index, item in enumerate(candidates):
            source = Path(discovery["input_dir"]) / item["relative_path"]
            (validation_dir / f"{index:04d}_{item['source_file']}").symlink_to(source)
        outcomes = _pipeline().validate_inputs(validation_dir)

    fatal = [
        outcome for outcome in outcomes
        if outcome.get("error") or outcome.get("missing_columns")
    ]
    if fatal:
        raise ValueError(f"File schema validation failed: {json.dumps(fatal, default=str)}")
    rejected = sum(int(outcome.get("rejected_rows", 0)) for outcome in outcomes)
    logger.info("Validated %d files; %d row(s) will be quarantined", len(outcomes), rejected)
    return {"validated_files": len(outcomes), "outcomes": outcomes, "rows_rejected": rejected}


def create_orchestration_batch_id(validation: dict[str, Any], run_id: str | None = None) -> dict[str, str]:
    """Generate a correlation ID for Airflow logs and audit output."""
    now = datetime.now(timezone.utc)
    safe_run_id = "".join(character if character.isalnum() else "_" for character in (run_id or "manual"))[-48:]
    return {
        "batch_id": f"af_{now.strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:10]}",
        "airflow_run_id": safe_run_id,
        "started_at": now.isoformat(),
        "validated_files": str(validation.get("validated_files", 0)),
    }


def ingest_candidates(discovery: dict[str, Any], batch_info: dict[str, str]) -> dict[str, Any]:
    """Ingest new files through the existing fingerprinted batch API."""
    pipeline = _pipeline()
    input_dir = Path(discovery["input_dir"])
    candidates = sorted(
        discovery["candidates"],
        key=lambda item: (SOURCE_ORDER[SourceType(item["source_type"])], item["source_file"].lower()),
    )
    results = []
    for item in candidates:
        result = pipeline.ingest_file(
            input_dir / item["relative_path"],
            source_type=item["source_type"],
            source_system=pipeline.settings.source_system,
            reprocess=bool(discovery.get("backfill")),
        )
        results.append(result.as_dict())
    totals = {
        "files_seen": len(candidates),
        "files_skipped": sum(bool(row.get("skipped")) for row in results),
        "rows_ingested": sum(int(row.get("rows_ingested", 0)) for row in results),
        "rows_processed": sum(int(row.get("rows_processed", 0)) for row in results),
        "rows_rejected": sum(int(row.get("rows_rejected", 0)) for row in results),
        "rows_loaded": sum(int(row.get("rows_loaded", 0)) for row in results),
    }
    logger.info("Airflow batch %s ingestion results: %s", batch_info["batch_id"], totals)
    return {"batch_id": batch_info["batch_id"], "files": results, **totals}


def verify_bronze(ingestion: dict[str, Any] | None = None) -> dict[str, Any]:
    """Confirm raw Bronze Parquet exists and return the measurable batch totals."""
    root = PROJECT_ROOT / "data" / "bronze"
    files = list(root.rglob("*.parquet")) if root.exists() else []
    if not files and ingestion and ingestion.get("rows_loaded", 0):
        raise FileNotFoundError("Ingestion reported loaded rows but Bronze Parquet is missing")
    return {"bronze_files": len(files), "rows_loaded": int((ingestion or {}).get("rows_loaded", 0))}


def run_spark_transformations(_bronze: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run the Phase 3 PySpark batch plus CDC Bronze transformation as a finite task."""
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": f"{PROJECT_ROOT / 'src'}{os.pathsep}{env.get('PYTHONPATH', '')}",
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
            "PYSPARK_SUBMIT_ARGS": (
                "--driver-memory 512m "
                "--conf spark.sql.shuffle.partitions=4 "
                "--conf spark.default.parallelism=4 "
                "--conf 'spark.driver.extraJavaOptions=-XX:ActiveProcessorCount=2 "
                "-XX:MaxMetaspaceSize=192m -XX:ReservedCodeCacheSize=64m "
                "-Xss512k -XX:MaxDirectMemorySize=128m' pyspark-shell"
            ),
        }
    )
    _run_command([sys.executable, "scripts/run_phase3_spark.py"], cwd=PROJECT_ROOT, env=env)
    return verify_silver()


def verify_silver(_spark_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Check the Phase 3 employee and history Parquet outputs."""
    quality_path = PROJECT_ROOT / "data" / "phase3" / "quality" / "quality_report.json"
    current_path = PROJECT_ROOT / "data" / "phase3" / "silver" / "employee_current"
    if not quality_path.is_file() or not current_path.exists():
        raise FileNotFoundError("Phase 3 Silver output or quality report is missing")
    report = json.loads(quality_path.read_text(encoding="utf-8"))
    return {
        "employee_rows": int(report["accepted_rows"]),
        "employee_history_rows": _parquet_count(PROJECT_ROOT / "data" / "phase3" / "silver" / "employee_history"),
        "dependent_rows": int(report["dependent_accepted_rows"]),
        "quality_report": str(quality_path),
    }


def verify_data_quality(_silver_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Fail the workflow only for broken reconciliation or missing quality outputs."""
    path = PROJECT_ROOT / "data" / "phase3" / "quality" / "quality_report.json"
    if not path.is_file():
        raise FileNotFoundError(f"Quality report not found: {path}")
    report = json.loads(path.read_text(encoding="utf-8"))
    rejected_records = int(report.get("employee_rejected_rows", 0)) + int(report.get("dependent_rejected_rows", 0))
    _record_local_metric("streamlake_data_quality", "data_quality_rejected_records", rejected_records,
                         {"report": str(path)})
    reconciliation_fields = (
        "source_target_reconciled",
        "employee_source_target_reconciled",
        "dependent_source_target_reconciled",
    )
    failed = [name for name in reconciliation_fields if report.get(name) is not True]
    if failed:
        _record_local_metric("streamlake_data_quality", "data_quality_failures", len(failed),
                             {"failed_checks": failed})
        send_alert("StreamLake data quality reconciliation failed", ", ".join(failed))
        raise ValueError(f"Data quality reconciliation failed: {failed}")
    return {
        "quality_report": report,
        "records_rejected": int(report.get("employee_rejected_rows", 0)) + int(report.get("dependent_rejected_rows", 0)),
        "employee_rows": int(report.get("accepted_rows", 0)),
        "dependent_rows": int(report.get("dependent_accepted_rows", 0)),
    }


def verify_gold(_quality_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Check Phase 1 Gold files and Phase 3 dimensional DuckDB tables."""
    gold_dir = PROJECT_ROOT / "data" / "gold"
    gold_files = list(gold_dir.glob("*.parquet")) if gold_dir.exists() else []
    database = PROJECT_ROOT / "data" / "streamlake_phase3.duckdb"
    table_counts: dict[str, int] = {}
    if database.is_file():
        connection = duckdb.connect(str(database), read_only=True)
        try:
            for table in ("dim_employee", "fact_employee_enrollment", "fact_employee_changes", "fact_policy_utilization"):
                table_counts[table] = int(connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0])
        finally:
            connection.close()
    return {"gold_files": len(gold_files), "mart_counts": table_counts}


def run_dbt_transformation(_gold_result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build all local dbt models and data tests."""
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(PROJECT_ROOT / "transform" / "dbt")
    result = _run_command(
        ["dbt", "build", "--target", "local"],
        cwd=PROJECT_ROOT / "transform" / "dbt",
        env=env,
    )
    return {"dbt_returncode": result.returncode, "target": "local", "gold": _gold_result or {}}


def write_audit_report(
    discovery: dict[str, Any],
    batch_info: dict[str, str],
    ingestion: dict[str, Any],
    quality: dict[str, Any],
    dbt_result: dict[str, Any],
    *,
    dag_id: str = "streamlake_batch_ingestion",
    run_id: str | None = None,
) -> dict[str, Any]:
    """Persist an orchestration audit with run timing and row metrics."""
    now = datetime.now(timezone.utc)
    started = datetime.fromisoformat(batch_info["started_at"])
    duration_seconds = max(0.0, (now - started).total_seconds())
    report = {
        "dag_id": dag_id,
        "airflow_run_id": run_id or batch_info.get("airflow_run_id"),
        "batch_id": batch_info["batch_id"],
        "started_at": started.isoformat(),
        "ended_at": now.isoformat(),
        "duration_seconds": round(duration_seconds, 3),
        "candidate_files": discovery.get("candidate_count", 0),
        "files_ingested": ingestion.get("files_seen", 0) - ingestion.get("files_skipped", 0),
        "files_skipped": ingestion.get("files_skipped", 0),
        "records_processed": ingestion.get("rows_processed", 0),
        "records_rejected": quality.get("records_rejected", 0),
        "employee_rows": quality.get("employee_rows", 0),
        "dbt": dbt_result,
        "quality_reconciliation": {
            field: quality["quality_report"].get(field)
            for field in (
                "source_target_reconciled",
                "employee_source_target_reconciled",
                "dependent_source_target_reconciled",
            )
        },
    }
    target = PROJECT_ROOT / "data" / "airflow" / "audit" / f"{batch_info['batch_id']}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    try:
        connection = duckdb.connect(str(PROJECT_ROOT / "data" / "streamlake.duckdb"))
        try:
            ensure_reliability_tables(connection)
            record_batch_audit(
                connection, execution_id=f"airflow:{dag_id}:{report['airflow_run_id'] or batch_info['batch_id']}",
                batch_id=batch_info["batch_id"], source_name=discovery.get("input_dir", "data/incoming"),
                start_time=started, end_time=now, status="completed",
                records_read=int(ingestion.get("rows_ingested", 0)),
                records_written=int(ingestion.get("rows_loaded", 0)),
                records_rejected=int(quality.get("records_rejected", 0)), pipeline_name=dag_id,
            )
            record_metric(connection, dag_id, "data_quality_rejected_records",
                          int(quality.get("records_rejected", 0)), {"batch_id": batch_info["batch_id"]})
            lineage_items = [
                ("airflow_bronze_load", "bronze", int(ingestion.get("rows_ingested", 0)),
                 int(ingestion.get("rows_ingested", 0)), 0),
                ("pyspark_bronze_to_silver", "silver.employees",
                 int(ingestion.get("rows_processed", 0)), int(quality.get("employee_rows", 0)),
                 int(quality.get("records_rejected", 0))),
                ("pyspark_bronze_to_silver", "silver.dependents",
                 int(ingestion.get("rows_processed", 0)), int(quality.get("dependent_rows", 0)),
                 int(quality.get("records_rejected", 0))),
            ]
            gold_counts = ((dbt_result.get("gold") or {}).get("mart_counts") or {})
            lineage_items.extend(
                ("dbt_transformation", f"gold.{table}", None, int(row_count), 0)
                for table, row_count in gold_counts.items()
            )
            for transformation, target, read_count, write_count, reject_count in lineage_items:
                record_lineage(
                    connection, execution_id=f"airflow:{dag_id}:{report['airflow_run_id'] or batch_info['batch_id']}",
                    batch_id=batch_info["batch_id"], source_system="streamlake",
                    source_name=discovery.get("input_dir", "data/incoming"),
                    transformation_name=transformation, target_table=target,
                    records_read=read_count, records_written=write_count, records_rejected=reject_count,
                )
        finally:
            connection.close()
    except Exception:
        logger.exception("Could not persist Airflow audit metrics in DuckDB")
    logger.info("Airflow audit report written to %s", target)
    return {"audit_path": str(target), "report": report}


def daily_reconciliation() -> dict[str, Any]:
    """Compare source batch outcomes, quality reconciliation, and marts each day."""
    pipeline = _pipeline()
    batch_report = pipeline.report()
    quality_path = PROJECT_ROOT / "data" / "phase3" / "quality" / "quality_report.json"
    if not quality_path.is_file():
        raise FileNotFoundError("Run the batch transformation before daily reconciliation")
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    mart = verify_gold()
    if not all(
        quality.get(key) is True
        for key in ("source_target_reconciled", "employee_source_target_reconciled", "dependent_source_target_reconciled")
    ):
        raise ValueError("Daily source-to-target reconciliation failed")
    report = {
        "reconciled_at": datetime.now(timezone.utc).isoformat(),
        "batch_report": batch_report,
        "quality": quality,
        "marts": mart,
    }
    target = PROJECT_ROOT / "data" / "airflow" / "reconciliation" / f"{date.today().isoformat()}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    logger.info("Daily reconciliation written to %s", target)
    return {"reconciliation_path": str(target), "marts": mart["mart_counts"]}


def failure_notification_callback(context: dict[str, Any]) -> None:
    """Persist and alert on the actual task failure, then run an optional callback."""
    task_instance = context.get("task_instance")
    dag_id = getattr(getattr(task_instance, "dag", None), "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = getattr(task_instance, "run_id", "unknown")
    message = {
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "exception": str(context.get("exception", "unknown")),
    }
    logger.error("Airflow task failed: %s", json.dumps(message, sort_keys=True))
    send_alert(f"StreamLake Airflow failure: {dag_id}.{task_id}", json.dumps(message, sort_keys=True))
    _record_airflow_event(context, "airflow_task_failures", "failed")
    callback_spec = os.environ.get("STREAMLAKE_AIRFLOW_FAILURE_CALLBACK", "").strip()
    if not callback_spec:
        return
    module_name, separator, function_name = callback_spec.partition(":")
    if not separator or not module_name or not function_name:
        logger.error("Invalid STREAMLAKE_AIRFLOW_FAILURE_CALLBACK; expected package.module:function")
        return
    try:
        callback = getattr(importlib.import_module(module_name), function_name)
        callback(context)
    except Exception:
        logger.exception("Configured Airflow failure callback failed")


def retry_notification_callback(context: dict[str, Any]) -> None:
    """Record each Airflow retry attempt from the task instance context."""
    task_instance = context.get("task_instance")
    logger.warning("Airflow task retry: dag=%s task=%s run=%s try=%s",
                   getattr(getattr(task_instance, "dag", None), "dag_id", "unknown"),
                   getattr(task_instance, "task_id", "unknown"),
                   getattr(task_instance, "run_id", "unknown"),
                   getattr(task_instance, "try_number", 0))
    _record_airflow_event(context, "airflow_task_retries", "retrying")


def _record_airflow_event(context: dict[str, Any], metric_name: str, status: str) -> None:
    task_instance = context.get("task_instance")
    dag_id = getattr(getattr(task_instance, "dag", None), "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = getattr(task_instance, "run_id", "unknown")
    now = datetime.now(timezone.utc)
    start = getattr(task_instance, "start_date", None) or now
    end = getattr(task_instance, "end_date", None)
    execution_id = f"airflow:{dag_id}:{run_id}:{task_id}:{getattr(task_instance, 'try_number', 0)}"
    try:
        database = PROJECT_ROOT / "data" / "streamlake.duckdb"
        database.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(database))
        try:
            ensure_reliability_tables(connection)
            record_batch_audit(connection, execution_id=execution_id, batch_id=str(run_id),
                               source_name=f"{dag_id}.{task_id}", start_time=start,
                               end_time=end, status=status,
                               error_message=str(context.get("exception", "")) or None,
                               pipeline_name=str(dag_id))
            record_metric(connection, str(dag_id), metric_name, 1,
                          {"task_id": task_id, "run_id": run_id,
                           "try_number": getattr(task_instance, "try_number", 0)})
        finally:
            connection.close()
    except Exception:
        logger.exception("Could not persist Airflow reliability event")


def _record_local_metric(pipeline_name: str, metric_name: str, value: float, dimensions: dict[str, Any]) -> None:
    try:
        database = PROJECT_ROOT / "data" / "streamlake.duckdb"
        database.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(database))
        try:
            ensure_reliability_tables(connection)
            record_metric(connection, pipeline_name, metric_name, value, dimensions)
        finally:
            connection.close()
    except Exception:
        logger.exception("Could not persist metric %s", metric_name)


def _parquet_count(path: Path) -> int:
    if not path.exists():
        return 0
    connection = duckdb.connect()
    try:
        glob = str(path / "**" / "*.parquet").replace("'", "''")
        return int(connection.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0])
    finally:
        connection.close()


def _run_command(command: list[str], *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess:
    logger.info("Running command %s in %s", command, cwd)
    try:
        result = subprocess.run(command, cwd=cwd, env=env, check=False, text=True)
    except Exception:
        logger.exception("Could not start command %s", command)
        raise
    if result.returncode:
        raise RuntimeError(f"Command {command[0]!r} failed with exit code {result.returncode}")
    return result
