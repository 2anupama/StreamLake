"""Local, chunked batch ingestion and medallion-layer orchestration."""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pandas as pd

from streamlake.common.settings import Settings
from streamlake.ingestion.batch.reader import iter_input_chunks, read_column_names, validate_extension
from streamlake.ingestion.batch.schemas import (
    DEPENDENT_COLUMNS,
    EMPLOYEE_COLUMNS,
    REQUIRED_COLUMNS,
    SourceType,
    infer_source_type,
)
from streamlake.ingestion.batch.validation import validate_chunk
from streamlake.monitoring.reliability import (
    ensure_reliability_tables,
    monitoring_snapshot,
    observe_schema,
    record_batch_audit,
    record_lineage,
    record_metric,
)
from streamlake.storage.adapters import create_storage_adapter

logger = logging.getLogger(__name__)
METADATA_COLUMNS = (
    "ingestion_timestamp",
    "batch_id",
    "source_file",
    "source_system",
    "source_type",
    "ingestion_status",
)
RUN_ORDER = {
    SourceType.ADDITIONS: 0,
    SourceType.UPDATES: 1,
    SourceType.DEPENDENTS: 2,
    SourceType.DELETIONS: 3,
}
RESERVED_COLUMNS = set(METADATA_COLUMNS)


@dataclass(frozen=True)
class IngestionResult:
    batch_id: str
    source_file: str
    source_type: str
    status: str
    rows_ingested: int = 0
    rows_processed: int = 0
    rows_rejected: int = 0
    rows_loaded: int = 0
    skipped: bool = False
    duplicate_of: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class BatchPipeline:
    """Orchestrate local file ingestion into Bronze, Silver, Gold, and quarantine."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.data_root = Path(self.settings.data_root)
        self.incoming_dir = self.data_root / "incoming"
        self.database_path = Path(self.settings.duckdb_path)
        self.storage = create_storage_adapter(self.settings)
        self.audit_path = Path(self.settings.ingestion_audit_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_database()

    def ingest_file(
        self,
        path: Path,
        source_type: str | SourceType | None = None,
        source_system: str | None = None,
        reprocess: bool = False,
    ) -> IngestionResult:
        """Ingest one file, returning zero loaded rows for an idempotent replay."""
        path = Path(path)
        extension = validate_extension(path)
        if not path.is_file():
            raise FileNotFoundError(path)
        resolved_source_type = infer_source_type(path, source_type)
        resolved_source_system = (source_system or self.settings.source_system).strip()
        if not resolved_source_system:
            raise ValueError("source_system must not be empty")
        fingerprint = self._fingerprint(path, resolved_source_type, resolved_source_system)

        with self._batch_lock():
            connection = self._connect()
            try:
                previous = connection.execute(
                    """SELECT batch_id, status FROM batch_history
                       WHERE file_fingerprint = ? AND status IN ('completed', 'empty')
                       ORDER BY started_at DESC LIMIT 1""",
                    [fingerprint],
                ).fetchone()
                if previous and not reprocess:
                    replay_batch_id = f"batch_{uuid.uuid4().hex}"
                    replay_started_at = self._now()
                    counters = {"ingested": 0, "processed": 0, "rejected": 0, "loaded": 0}
                    self._start_batch(
                        connection,
                        replay_batch_id,
                        fingerprint,
                        path.name,
                        resolved_source_system,
                        resolved_source_type,
                        replay_started_at,
                    )
                    self._finish_batch(
                        connection, replay_batch_id, "skipped_duplicate", counters, None
                    )
                    connection.execute(
                        "UPDATE batch_history SET duplicate_of = ? WHERE batch_id = ?",
                        [previous[0], replay_batch_id],
                    )
                    result = IngestionResult(
                        batch_id=replay_batch_id,
                        source_file=path.name,
                        source_type=resolved_source_type.value,
                        status="skipped_duplicate",
                        skipped=True,
                        duplicate_of=previous[0],
                    )
                    record_metric(connection, "streamlake_batch_ingestion", "duplicate_batches", 1,
                                  {"duplicate_of": previous[0], "source_file": path.name})
                    self._write_audit(result, resolved_source_system, replay_started_at)
                    return result

                batch_id = f"batch_{uuid.uuid4().hex}"
                started_at = self._now()
                ingestion_date = started_at[:10]
                self._start_batch(
                    connection,
                    batch_id,
                    fingerprint,
                    path.name,
                    resolved_source_system,
                    resolved_source_type,
                    started_at,
                )
                counters = {"ingested": 0, "processed": 0, "rejected": 0, "loaded": 0}
                try:
                    columns = read_column_names(path)
                    if not columns:
                        self._finish_batch(connection, batch_id, "empty", counters, None)
                        result = self._result(batch_id, path.name, resolved_source_type, "empty", counters)
                        self._write_audit(result, resolved_source_system, started_at)
                        return result

                    missing_columns = set(REQUIRED_COLUMNS[resolved_source_type]) - set(columns)
                    reserved_conflicts = RESERVED_COLUMNS & set(columns)
                    schema_types = self._infer_schema_types(path, columns)
                    schema_observation = observe_schema(
                        connection, resolved_source_type.value, columns, schema_types,
                        REQUIRED_COLUMNS[resolved_source_type],
                    )
                    if schema_observation["added"]:
                        record_metric(connection, "streamlake_batch_ingestion", "schema_columns_added",
                                      len(schema_observation["added"]), {"source_type": resolved_source_type.value})
                    if schema_observation["removed"]:
                        record_metric(connection, "streamlake_batch_ingestion", "schema_columns_removed",
                                      len(schema_observation["removed"]), {"source_type": resolved_source_type.value})
                    if missing_columns:
                        metadata = {
                            "ingestion_timestamp": self._now(), "batch_id": batch_id,
                            "source_file": path.name, "source_system": resolved_source_system,
                            "source_type": resolved_source_type.value, "ingestion_status": "schema_incompatible",
                        }
                        message = f"Missing required columns: {', '.join(sorted(missing_columns))}"
                        self._write_quarantine(
                            [{"original_record": json.dumps({"columns": columns}), "error_reason": message}],
                            metadata, ingestion_date, 0,
                        )
                        counters["rejected"] += 1
                        raise ValueError(
                            message
                        )
                    if reserved_conflicts:
                        raise ValueError(
                            "Input contains reserved ingestion metadata columns: "
                            + ", ".join(sorted(reserved_conflicts))
                        )

                    processing_timestamp = self._now()
                    all_employee_ids, active_employee_ids, dependent_ids = self._reference_ids(connection)
                    seen_ids: set[str] = set()
                    part_number = 0
                    for chunk in iter_input_chunks(path, self.settings.batch_chunk_size):
                        counters["ingested"] += len(chunk.frame) + len(chunk.malformed_rows)
                        metadata = {
                            "ingestion_timestamp": processing_timestamp,
                            "batch_id": batch_id,
                            "source_file": path.name,
                            "source_system": resolved_source_system,
                            "source_type": resolved_source_type.value,
                            "ingestion_status": "ingested",
                        }
                        if not chunk.frame.empty:
                            bronze = chunk.frame.copy()
                            for name, value in metadata.items():
                                bronze[name] = value
                            self.storage.write_partitioned_frame(
                                bronze,
                                "bronze",
                                ingestion_date,
                                resolved_source_type.value,
                                batch_id,
                                part_number,
                            )
                            record_lineage(
                                connection, execution_id=batch_id, batch_id=batch_id,
                                source_system=resolved_source_system, source_name=path.name,
                                transformation_name="raw_ingestion", target_table="bronze",
                                records_read=len(chunk.frame), records_written=len(bronze),
                            )
                            part_number += 1

                            validation = validate_chunk(
                                chunk.frame,
                                resolved_source_type,
                                all_employee_ids=all_employee_ids,
                                active_employee_ids=active_employee_ids,
                                existing_dependent_ids=dependent_ids,
                                seen_ids=seen_ids,
                                allow_existing_ids=all_employee_ids if reprocess else None,
                                allow_existing_dependent_ids=dependent_ids if reprocess else None,
                            )
                            accepted_rows = validation.accepted
                            rejected_rows = validation.rejected
                            incompatible_types = schema_observation["incompatible_columns"]
                            if incompatible_types and accepted_rows:
                                rejected_rows = rejected_rows + [
                                    {**row, "original_record": json.dumps(row, default=str),
                                     "error_reason": "Incompatible schema type change: " + ", ".join(incompatible_types)}
                                    for row in accepted_rows
                                ]
                                accepted_rows = []
                            counters["processed"] += len(accepted_rows)
                            counters["rejected"] += len(rejected_rows)
                            if accepted_rows:
                                accepted = self._prepare_accepted(
                                    connection,
                                    accepted_rows,
                                    resolved_source_type,
                                    metadata,
                                    ingestion_date,
                                    part_number,
                                )
                                counters["loaded"] += self._apply_records(
                                    connection, accepted, resolved_source_type
                                )
                                if resolved_source_type is SourceType.ADDITIONS:
                                    for record in accepted:
                                        employee_id = str(record["employee_id"])
                                        all_employee_ids.add(employee_id)
                                        active_employee_ids.add(employee_id)
                                elif resolved_source_type is SourceType.UPDATES:
                                    for record in accepted:
                                        all_employee_ids.add(str(record["employee_id"]))
                                        active_employee_ids.add(str(record["employee_id"]))
                                elif resolved_source_type is SourceType.DELETIONS:
                                    for record in accepted:
                                        active_employee_ids.discard(str(record["employee_id"]))
                                else:
                                    dependent_ids.update(str(record["dependent_id"]) for record in accepted)

                                silver = pd.DataFrame.from_records(accepted)
                                self.storage.write_partitioned_frame(
                                    silver,
                                    "silver/events",
                                    ingestion_date,
                                    resolved_source_type.value,
                                    batch_id,
                                    part_number,
                                )
                                record_lineage(
                                    connection, execution_id=batch_id, batch_id=batch_id,
                                    source_system=resolved_source_system, source_name=path.name,
                                    transformation_name="validate_and_normalize", target_table="silver_events",
                                    records_read=len(accepted_rows), records_written=len(accepted),
                                )
                                part_number += 1

                            if rejected_rows:
                                self._write_quarantine(
                                    rejected_rows,
                                    metadata,
                                    ingestion_date,
                                    part_number,
                                )
                                record_lineage(
                                    connection, execution_id=batch_id, batch_id=batch_id,
                                    source_system=resolved_source_system, source_name=path.name,
                                    transformation_name="schema_and_row_validation", target_table="quarantine",
                                    records_read=len(chunk.frame), records_rejected=len(rejected_rows),
                                )
                                part_number += 1

                        if chunk.malformed_rows:
                            counters["rejected"] += len(chunk.malformed_rows)
                            malformed_rows = [
                                {
                                    "original_record": json.dumps(
                                        {"raw_fields": list(fields)}, ensure_ascii=True
                                    ),
                                    "error_reason": "Malformed CSV row has more fields than the header",
                                }
                                for fields in chunk.malformed_rows
                            ]
                            malformed_bronze = pd.DataFrame(
                                {"raw_fields": [json.dumps(list(fields)) for fields in chunk.malformed_rows]}
                            )
                            for name, value in {**metadata, "ingestion_status": "malformed"}.items():
                                malformed_bronze[name] = value
                            self.storage.write_partitioned_frame(
                                malformed_bronze,
                                "bronze/malformed",
                                ingestion_date,
                                resolved_source_type.value,
                                batch_id,
                                part_number,
                            )
                            self._write_quarantine(
                                malformed_rows,
                                metadata,
                                ingestion_date,
                                part_number + 1,
                            )
                            record_lineage(
                                connection, execution_id=batch_id, batch_id=batch_id,
                                source_system=resolved_source_system, source_name=path.name,
                                transformation_name="malformed_record_quarantine", target_table="quarantine",
                                records_read=len(chunk.malformed_rows), records_rejected=len(chunk.malformed_rows),
                            )
                            part_number += 2

                    status = "completed" if counters["ingested"] else "empty"
                    self._refresh_layers(connection, batch_id=batch_id, source_file=path.name,
                                         source_system=resolved_source_system)
                    self._finish_batch(connection, batch_id, status, counters, None)
                    result = self._result(batch_id, path.name, resolved_source_type, status, counters)
                    self._write_audit(result, resolved_source_system, started_at)
                    return result
                except Exception as error:
                    self._finish_batch(connection, batch_id, "failed", counters, str(error))
                    failed = self._result(
                        batch_id, path.name, resolved_source_type, "failed", counters
                    )
                    self._write_audit(failed, resolved_source_system, started_at, str(error))
                    raise
            finally:
                connection.close()

    def run_batch(self, input_dir: Path | None = None) -> list[IngestionResult]:
        """Process conventional input files in dependency-safe order."""
        directory = Path(input_dir or self.incoming_dir)
        if not directory.is_dir():
            raise FileNotFoundError(f"Input directory does not exist: {directory}")
        candidates: list[tuple[int, Path, SourceType]] = []
        for path in directory.iterdir():
            if not path.is_file() or path.suffix.lower() not in {".csv", ".xlsx", ".xlsm", ".xls"}:
                continue
            try:
                source_type = infer_source_type(path)
            except ValueError:
                logger.warning("Skipping unrecognized input file %s", path.name)
                continue
            candidates.append((RUN_ORDER[source_type], path, source_type))
        candidates.sort(key=lambda item: (item[0], item[1].name.lower()))
        return [self.ingest_file(path, source_type) for _, path, source_type in candidates]

    def validate_inputs(self, input_dir: Path | None = None) -> list[dict[str, Any]]:
        """Stream source files through schema and row-level validation without writing."""
        directory = Path(input_dir or self.incoming_dir)
        if not directory.is_dir():
            raise FileNotFoundError(f"Input directory does not exist: {directory}")
        outcomes: list[dict[str, Any]] = []
        connection = self._connect()
        try:
            all_employee_ids, active_employee_ids, dependent_ids = self._reference_ids(connection)
            paths: list[tuple[int, Path, SourceType]] = []
            for path in directory.iterdir():
                if not path.is_file() or path.name.startswith("."):
                    continue
                try:
                    validate_extension(path)
                    source_type = infer_source_type(path)
                    paths.append((RUN_ORDER[source_type], path, source_type))
                except ValueError as error:
                    outcomes.append({"file": path.name, "status": "invalid", "error": str(error)})
            paths.sort(key=lambda item: (item[0], item[1].name.lower()))

            for _, path, source_type in paths:
                try:
                    validate_extension(path)
                    columns = read_column_names(path)
                    if not columns:
                        outcomes.append(
                            {
                                "file": path.name,
                                "source_type": source_type.value,
                                "status": "empty",
                                "rows": 0,
                                "valid_rows": 0,
                                "rejected_rows": 0,
                                "missing_columns": [],
                            }
                        )
                        continue

                    missing = sorted(set(REQUIRED_COLUMNS[source_type]) - set(columns))
                    seen_ids: set[str] = set()
                    row_count = 0
                    valid_count = 0
                    rejected_count = 0
                    for chunk in iter_input_chunks(path, self.settings.batch_chunk_size):
                        row_count += len(chunk.frame) + len(chunk.malformed_rows)
                        rejected_count += len(chunk.malformed_rows)
                        result = validate_chunk(
                            chunk.frame,
                            source_type,
                            all_employee_ids=all_employee_ids,
                            active_employee_ids=active_employee_ids,
                            existing_dependent_ids=dependent_ids,
                            seen_ids=seen_ids,
                        )
                        valid_count += len(result.accepted)
                        rejected_count += len(result.rejected)
                        if source_type is SourceType.ADDITIONS:
                            for record in result.accepted:
                                employee_id = str(record["employee_id"])
                                all_employee_ids.add(employee_id)
                                active_employee_ids.add(employee_id)
                        elif source_type is SourceType.UPDATES:
                            for record in result.accepted:
                                all_employee_ids.add(str(record["employee_id"]))
                                active_employee_ids.add(str(record["employee_id"]))
                        elif source_type is SourceType.DELETIONS:
                            for record in result.accepted:
                                active_employee_ids.discard(str(record["employee_id"]))
                        else:
                            dependent_ids.update(
                                str(record["dependent_id"]) for record in result.accepted
                            )

                    status = "invalid" if missing or rejected_count else "valid"
                    if row_count == 0 and not missing:
                        status = "empty"
                    outcomes.append(
                        {
                            "file": path.name,
                            "source_type": source_type.value,
                            "status": status,
                            "rows": row_count,
                            "valid_rows": valid_count,
                            "rejected_rows": rejected_count,
                            "missing_columns": missing,
                        }
                    )
                except (OSError, ValueError, ImportError) as error:
                    outcomes.append({"file": path.name, "status": "invalid", "error": str(error)})
        finally:
            connection.close()
        return outcomes

    def reprocess_batch(self, batch_id: str) -> IngestionResult:
        """Reprocess one completed source batch; business-key upserts stay idempotent."""
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT source_file, source_type, source_system, file_fingerprint "
                "FROM batch_history WHERE batch_id = ?",
                [batch_id],
            ).fetchone()
        finally:
            connection.close()
        if not row:
            raise ValueError(f"Unknown batch_id: {batch_id}")
        source = self.incoming_dir / row[0]
        if not source.is_file():
            raise FileNotFoundError(f"Backfill source file is no longer available: {source}")
        current_fingerprint = self._fingerprint(source, SourceType(row[1]), row[2])
        if current_fingerprint != row[3]:
            raise ValueError(f"Source file {source.name!r} no longer matches batch {batch_id}; restore its archived bytes")
        return self.ingest_file(source, row[1], source_system=row[2], reprocess=True)

    def rebuild_silver(self) -> dict[str, int]:
        """Rebuild current Silver from retained accepted event Parquet partitions."""
        if self.settings.storage_backend != "local":
            raise RuntimeError("Silver rebuild from retained event files currently requires STORAGE_BACKEND=local")
        event_root = self.data_root / "silver" / "events"
        files = sorted(event_root.rglob("*.parquet")) if event_root.exists() else []
        if not files:
            raise FileNotFoundError(f"No retained Silver event Parquet files under {event_root}")
        glob = str(event_root / "**" / "*.parquet").replace("'", "''")
        dependent_files = [path for path in files if "source_type=dependents" in path.as_posix()]
        dependent_glob = str(event_root / "**" / "source_type=dependents" / "**" / "*.parquet").replace("'", "''")
        connection = self._connect()
        execution_id = f"rebuild-silver-{uuid.uuid4().hex}"
        started = datetime.now(timezone.utc)
        try:
            available_columns = {
                row[0] for row in connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{glob}', union_by_name=true)"
                ).fetchall()
            }
            ingest_order = ("try_cast(ingestion_timestamp AS TIMESTAMP)" if "ingestion_timestamp" in available_columns
                            else "NULL::TIMESTAMP")
            batch_order = "batch_id" if "batch_id" in available_columns else "''"
            connection.execute("BEGIN TRANSACTION")
            connection.execute(
                f"""CREATE OR REPLACE TEMP TABLE rebuilt_employee_master AS
                    SELECT {', '.join(f'"{column}"' for column in EMPLOYEE_COLUMNS)}
                    FROM read_parquet('{glob}', union_by_name=true)
                    WHERE coalesce(source_type, '') <> 'dependents'
                    QUALIFY row_number() OVER (PARTITION BY employee_id ORDER BY
                        try_cast(updated_at AS TIMESTAMP) DESC NULLS LAST,
                        {ingest_order} DESC NULLS LAST,
                        {batch_order} DESC) = 1"""
            )
            if dependent_files:
                connection.execute(
                    f"""CREATE OR REPLACE TEMP TABLE rebuilt_employee_dependents AS
                        SELECT {', '.join(f'"{column}"' for column in DEPENDENT_COLUMNS)}
                        FROM read_parquet('{dependent_glob}', union_by_name=true)
                        QUALIFY row_number() OVER (PARTITION BY dependent_id ORDER BY
                            try_cast(updated_at AS TIMESTAMP) DESC NULLS LAST,
                            {ingest_order} DESC NULLS LAST,
                            {batch_order} DESC) = 1"""
                )
            connection.execute("DELETE FROM silver_employee_master")
            connection.execute(
                f"INSERT INTO silver_employee_master ({', '.join(EMPLOYEE_COLUMNS)}) "
                f"SELECT {', '.join(EMPLOYEE_COLUMNS)} FROM rebuilt_employee_master"
            )
            if dependent_files:
                connection.execute("DELETE FROM silver_employee_dependents")
                connection.execute(
                    f"INSERT INTO silver_employee_dependents ({', '.join(DEPENDENT_COLUMNS)}) "
                    f"SELECT {', '.join(DEPENDENT_COLUMNS)} FROM rebuilt_employee_dependents"
                )
            employees = int(connection.execute("SELECT count(*) FROM silver_employee_master").fetchone()[0])
            dependents = int(connection.execute("SELECT count(*) FROM silver_employee_dependents").fetchone()[0])
            connection.execute("COMMIT")
            record_lineage(connection, execution_id=execution_id, batch_id=None,
                           source_system="local_parquet", source_name=str(event_root),
                           transformation_name="rebuild_silver", target_table="silver_employee_master",
                           records_read=len(files), records_written=employees)
            record_lineage(connection, execution_id=execution_id, batch_id=None,
                           source_system="local_parquet", source_name=str(event_root),
                           transformation_name="rebuild_silver", target_table="silver_employee_dependents",
                           records_read=len(files), records_written=dependents)
            finished = datetime.now(timezone.utc)
            record_batch_audit(connection, execution_id=execution_id, batch_id=execution_id,
                               source_name=str(event_root), start_time=started, end_time=finished,
                               status="completed", records_read=len(files),
                               records_written=employees + dependents)
            self._refresh_layers(connection, batch_id=execution_id, source_file=str(event_root),
                                 source_system="local_parquet")
            return {"employee_rows": employees, "dependent_rows": dependents, "event_files": len(files)}
        except Exception as error:
            try:
                connection.execute("ROLLBACK")
            except duckdb.Error:
                pass
            finished = datetime.now(timezone.utc)
            record_batch_audit(connection, execution_id=execution_id, batch_id=execution_id,
                               source_name=str(event_root), start_time=started, end_time=finished,
                               status="failed", error_message=str(error))
            raise
        finally:
            connection.close()

    def rebuild_gold(self) -> dict[str, int]:
        """Regenerate Gold from current Silver tables and refresh Parquet snapshots."""
        connection = self._connect()
        execution_id = f"rebuild-gold-{uuid.uuid4().hex}"
        started = datetime.now(timezone.utc)
        try:
            self._refresh_layers(connection, batch_id=execution_id, source_file="Silver state",
                                 source_system="duckdb")
            result = {
                table: int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                for table in ("gold_employee_master", "gold_employee_dependents",
                              "gold_company_employee_summary", "gold_policy_summary")
            }
            record_batch_audit(connection, execution_id=execution_id, batch_id=execution_id,
                               source_name="Silver state", start_time=started,
                               end_time=datetime.now(timezone.utc), status="completed",
                               records_read=sum(result.values()), records_written=sum(result.values()))
            return result
        except Exception as error:
            record_batch_audit(connection, execution_id=execution_id, batch_id=execution_id,
                               source_name="Silver state", start_time=started,
                               end_time=datetime.now(timezone.utc), status="failed", error_message=str(error))
            raise
        finally:
            connection.close()

    def report(self) -> dict[str, Any]:
        """Return current layer sizes and the most recent batch outcomes."""
        connection = self._connect()
        try:
            employee_count = connection.execute(
                "SELECT count(*) FROM silver_employee_master WHERE record_status = 'active'"
            ).fetchone()[0]
            dependent_count = connection.execute(
                """SELECT count(*) FROM silver_employee_dependents AS dependent
                   INNER JOIN silver_employee_master AS employee USING (employee_id)
                   WHERE dependent.record_status = 'active' AND employee.record_status = 'active'"""
            ).fetchone()[0]
            batches = connection.execute(
                """SELECT batch_id, source_file, source_type, status, rows_ingested,
                          rows_processed, rows_rejected, rows_loaded, duplicate_of
                   FROM batch_history ORDER BY started_at DESC LIMIT 10"""
            ).fetchdf()
            return {
                "active_employees": int(employee_count),
                "active_dependents": int(dependent_count),
                "recent_batches": batches.to_dict(orient="records"),
                "duckdb_path": str(self.database_path),
                "monitoring": monitoring_snapshot(connection),
            }
        finally:
            connection.close()

    def _initialize_database(self) -> None:
        connection = self._connect()
        try:
            employee_definitions = []
            for column in EMPLOYEE_COLUMNS:
                data_type = "BIGINT" if column == "sum_insured" else "VARCHAR"
                primary_key = " PRIMARY KEY" if column == "employee_id" else ""
                employee_definitions.append(f"{column} {data_type}{primary_key}")
            dependent_definitions = []
            for column in DEPENDENT_COLUMNS:
                data_type = "BIGINT" if column == "sum_insured" else "VARCHAR"
                primary_key = " PRIMARY KEY" if column == "dependent_id" else ""
                dependent_definitions.append(f"{column} {data_type}{primary_key}")
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS silver_employee_master ({', '.join(employee_definitions)})"
            )
            connection.execute(
                f"CREATE TABLE IF NOT EXISTS silver_employee_dependents ({', '.join(dependent_definitions)})"
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS batch_history (
                    batch_id VARCHAR PRIMARY KEY,
                    file_fingerprint VARCHAR NOT NULL,
                    source_file VARCHAR NOT NULL,
                    source_system VARCHAR NOT NULL,
                    source_type VARCHAR NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    status VARCHAR NOT NULL,
                    duplicate_of VARCHAR,
                    rows_ingested BIGINT DEFAULT 0,
                    rows_processed BIGINT DEFAULT 0,
                    rows_rejected BIGINT DEFAULT 0,
                    rows_loaded BIGINT DEFAULT 0,
                    error_message VARCHAR
                )"""
            )
            connection.execute(
                "ALTER TABLE batch_history ADD COLUMN IF NOT EXISTS duplicate_of VARCHAR"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS batch_history_fingerprint_idx ON batch_history(file_fingerprint)"
            )
            ensure_reliability_tables(connection)
        finally:
            connection.close()

    def _connect(self):
        return duckdb.connect(str(self.database_path))

    @contextmanager
    def _batch_lock(self) -> Iterator[None]:
        lock_path = self.data_root / ".streamlake-batch.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _fingerprint(path: Path, source_type: SourceType, source_system: str) -> str:
        digest = hashlib.sha256(f"{source_type.value}\0{source_system}\0".encode())
        with path.open("rb") as source:
            for block in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def _infer_schema_types(self, path: Path, columns: list[str]) -> dict[str, str]:
        """Infer a bounded sample type profile without reading the full source twice."""
        sample = next(iter_input_chunks(path, min(self.settings.batch_chunk_size, 250)), None)
        frame = sample.frame if sample is not None else pd.DataFrame(columns=columns)
        types: dict[str, str] = {}
        id_columns = {column for column in columns if column.endswith("_id") or column == "id"}
        date_columns = {"date_of_birth", "joining_date", "effective_start_date", "effective_end_date"}
        timestamp_columns = {"created_at", "updated_at", "ingestion_timestamp"}
        for column in columns:
            values = frame[column].dropna().astype("string").str.strip()
            values = values[values.ne("")]
            if not len(values):
                types[column] = "NULL"
            elif column in id_columns:
                types[column] = "STRING"
            elif column in timestamp_columns:
                parsed = pd.to_datetime(values, errors="coerce", format="mixed")
                types[column] = "TIMESTAMP" if parsed.notna().all() else "STRING"
            elif column in date_columns:
                parsed = pd.to_datetime(values, errors="coerce", format="mixed")
                types[column] = "DATE" if parsed.notna().all() else "STRING"
            else:
                numeric = pd.to_numeric(values.str.replace(",", "", regex=False), errors="coerce")
                if numeric.notna().all():
                    types[column] = "INT64" if (numeric % 1 == 0).all() else "NUMERIC"
                elif numeric.notna().any():
                    types[column] = "MIXED"
                else:
                    types[column] = "STRING"
        return types

    def _start_batch(
        self,
        connection,
        batch_id: str,
        fingerprint: str,
        source_file: str,
        source_system: str,
        source_type: SourceType,
        started_at: str,
    ) -> None:
        connection.execute(
            """INSERT INTO batch_history (
                batch_id, file_fingerprint, source_file, source_system, source_type,
                started_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'running')""",
            [
                batch_id,
                fingerprint,
                source_file,
                source_system,
                source_type.value,
                datetime.fromisoformat(started_at),
            ],
        )
        parsed_start = datetime.fromisoformat(started_at)
        record_batch_audit(
            connection, execution_id=batch_id, batch_id=batch_id, source_name=source_file,
            start_time=parsed_start, end_time=None, status="running",
        )

    def _finish_batch(
        self,
        connection,
        batch_id: str,
        status: str,
        counters: dict[str, int],
        error_message: str | None,
    ) -> None:
        connection.execute(
            """UPDATE batch_history SET completed_at = ?, status = ?, rows_ingested = ?,
                rows_processed = ?, rows_rejected = ?, rows_loaded = ?, error_message = ?
               WHERE batch_id = ?""",
            [
                datetime.now(timezone.utc),
                status,
                counters["ingested"],
                counters["processed"],
                counters["rejected"],
                counters["loaded"],
                error_message,
                batch_id,
            ],
        )
        batch = connection.execute(
            "SELECT source_file, started_at, rows_ingested, rows_loaded, rows_rejected "
            "FROM batch_history WHERE batch_id = ?", [batch_id]
        ).fetchone()
        if batch:
            record_batch_audit(
                connection, execution_id=batch_id, batch_id=batch_id, source_name=batch[0],
                start_time=batch[1], end_time=datetime.now(timezone.utc), status=status,
                records_read=int(batch[2]), records_written=int(batch[3]),
                records_rejected=int(batch[4]), error_message=error_message,
            )

    def _reference_ids(self, connection) -> tuple[set[str], set[str], set[str]]:
        all_employee_ids = {
            row[0]
            for row in connection.execute("SELECT employee_id FROM silver_employee_master").fetchall()
        }
        active_employee_ids = {
            row[0]
            for row in connection.execute(
                "SELECT employee_id FROM silver_employee_master WHERE record_status = 'active'"
            ).fetchall()
        }
        dependent_ids = {
            row[0]
            for row in connection.execute("SELECT dependent_id FROM silver_employee_dependents").fetchall()
        }
        return all_employee_ids, active_employee_ids, dependent_ids

    def _prepare_accepted(
        self,
        connection,
        rows: list[dict[str, Any]],
        source_type: SourceType,
        metadata: dict[str, str],
        ingestion_date: str,
        part_number: int,
    ) -> list[dict[str, Any]]:
        if source_type is not SourceType.DELETIONS:
            return [{**row, **metadata} for row in rows]
        full_rows: list[dict[str, Any]] = []
        for deletion in rows:
            current = connection.execute(
                "SELECT * FROM silver_employee_master WHERE employee_id = ?",
                [deletion["employee_id"]],
            ).fetchone()
            if current is None:
                continue
            employee = dict(zip(EMPLOYEE_COLUMNS, current))
            employee["record_status"] = "deleted"
            employee["updated_at"] = deletion.get("updated_at") or self._now()
            full_rows.append({**employee, **metadata})
        return full_rows

    def _apply_records(self, connection, rows: list[dict[str, Any]], source_type: SourceType) -> int:
        if source_type is SourceType.DEPENDENTS:
            columns = DEPENDENT_COLUMNS
            table = "silver_employee_dependents"
        else:
            columns = EMPLOYEE_COLUMNS
            table = "silver_employee_master"
        frame = pd.DataFrame.from_records(rows, columns=columns)
        relation_name = f"stage_{uuid.uuid4().hex}"
        connection.register(relation_name, frame)
        try:
            selected_columns = ", ".join(columns)
            key = "dependent_id" if source_type is SourceType.DEPENDENTS else "employee_id"
            assignments = ", ".join(f"{column}=excluded.{column}" for column in columns if column != key)
            current_time = (
                f"coalesce(try_cast({table}.updated_at AS TIMESTAMP), "
                f"try_cast({table}.created_at AS TIMESTAMP), TIMESTAMP '1970-01-01 00:00:00')"
            )
            incoming_time = (
                "coalesce(try_cast(excluded.updated_at AS TIMESTAMP), "
                "try_cast(excluded.created_at AS TIMESTAMP), TIMESTAMP '1970-01-01 00:00:00')"
            )
            condition = f"{incoming_time} > {current_time} OR {incoming_time} = {current_time}"
            if source_type is not SourceType.DEPENDENTS:
                condition = (
                    f"{incoming_time} > {current_time} OR "
                    f"({incoming_time} = {current_time} AND "
                    f"(excluded.record_status = 'deleted' OR {table}.record_status <> 'deleted'))"
                )
            connection.execute(
                f"INSERT INTO {table} ({selected_columns}) "
                f"SELECT {selected_columns} FROM {relation_name} "
                f"ON CONFLICT ({key}) DO UPDATE SET {assignments} WHERE {condition} "
                f"RETURNING {key}"
            )
            return len(connection.fetchall())
        finally:
            connection.unregister(relation_name)

    def _write_quarantine(
        self,
        rows: list[dict[str, str]],
        metadata: dict[str, str],
        ingestion_date: str,
        part_number: int,
    ) -> None:
        processing_timestamp = self._now()
        frame = pd.DataFrame.from_records(
            [
                {
                    **row,
                    **metadata,
                    "processing_timestamp": processing_timestamp,
                }
                for row in rows
            ]
        )
        self.storage.write_partitioned_frame(
            frame,
            "quarantine",
            ingestion_date,
            metadata["source_type"],
            metadata["batch_id"],
            part_number,
        )

    def _refresh_layers(self, connection, batch_id: str | None = None,
                        source_file: str | None = None, source_system: str | None = None) -> None:
        connection.execute(
            """CREATE OR REPLACE TABLE gold_employee_master AS
               SELECT * FROM silver_employee_master WHERE record_status = 'active'"""
        )
        connection.execute(
            """CREATE OR REPLACE TABLE gold_employee_dependents AS
               SELECT dependent.* FROM silver_employee_dependents AS dependent
               INNER JOIN silver_employee_master AS employee USING (employee_id)
               WHERE dependent.record_status = 'active' AND employee.record_status = 'active'"""
        )
        connection.execute(
            """CREATE OR REPLACE TABLE gold_company_employee_summary AS
               SELECT company_id, company_name, count(*) AS employee_count,
                      count(DISTINCT policy_id) AS policy_count,
                      sum(sum_insured) AS total_sum_insured
               FROM gold_employee_master GROUP BY company_id, company_name"""
        )
        connection.execute(
            """CREATE OR REPLACE TABLE gold_policy_summary AS
               SELECT policy_id, company_id, company_name, count(*) AS employee_count,
                      sum(sum_insured) AS total_sum_insured
               FROM gold_employee_master GROUP BY policy_id, company_id, company_name"""
        )
        exports = {
            "silver_employee_master": "silver/silver_employee_master.parquet",
            "silver_employee_dependents": "silver/silver_employee_dependents.parquet",
            "gold_employee_master": "gold/gold_employee_master.parquet",
            "gold_employee_dependents": "gold/gold_employee_dependents.parquet",
            "gold_company_employee_summary": "gold/gold_company_employee_summary.parquet",
            "gold_policy_summary": "gold/gold_policy_summary.parquet",
        }
        for table, relative_path in exports.items():
            self.storage.write_query_parquet(
                connection,
                f"SELECT * FROM {table}",
                Path(relative_path),
            )
            row_count = int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            record_lineage(
                connection, execution_id=batch_id, batch_id=batch_id,
                source_system=source_system or "duckdb", source_name=source_file or table,
                transformation_name="refresh_analytical_snapshot", target_table=table,
                records_read=row_count, records_written=row_count,
            )

    def _write_audit(
        self,
        result: IngestionResult,
        source_system: str,
        started_at: str,
        error: str | None = None,
    ) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_record = {
            **result.as_dict(),
            "source_system": source_system,
            "started_at": started_at,
            "completed_at": self._now(),
            "error_message": error,
        }
        with self.audit_path.open("a", encoding="utf-8") as audit_file:
            audit_file.write(json.dumps(audit_record, ensure_ascii=True) + "\n")

    @staticmethod
    def _result(
        batch_id: str,
        source_file: str,
        source_type: SourceType,
        status: str,
        counters: dict[str, int],
    ) -> IngestionResult:
        return IngestionResult(
            batch_id=batch_id,
            source_file=source_file,
            source_type=source_type.value,
            status=status,
            rows_ingested=counters["ingested"],
            rows_processed=counters["processed"],
            rows_rejected=counters["rejected"],
            rows_loaded=counters["loaded"],
        )
