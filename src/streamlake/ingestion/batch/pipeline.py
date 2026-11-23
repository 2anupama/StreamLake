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
from streamlake.storage.adapters import LocalParquetStorage

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
        self.storage = LocalParquetStorage(self.data_root)
        self.audit_path = Path(self.settings.ingestion_audit_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_database()

    def ingest_file(
        self,
        path: Path,
        source_type: str | SourceType | None = None,
        source_system: str | None = None,
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
                if previous:
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
                    if missing_columns:
                        raise ValueError(
                            f"Missing required columns: {', '.join(sorted(missing_columns))}"
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
                            part_number += 1

                            validation = validate_chunk(
                                chunk.frame,
                                resolved_source_type,
                                all_employee_ids=all_employee_ids,
                                active_employee_ids=active_employee_ids,
                                existing_dependent_ids=dependent_ids,
                                seen_ids=seen_ids,
                            )
                            counters["processed"] += len(validation.accepted)
                            counters["rejected"] += len(validation.rejected)
                            if validation.accepted:
                                accepted = self._prepare_accepted(
                                    connection,
                                    validation.accepted,
                                    resolved_source_type,
                                    metadata,
                                    ingestion_date,
                                    part_number,
                                )
                                counters["loaded"] += len(accepted)
                                self._apply_records(connection, accepted, resolved_source_type)
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
                                part_number += 1

                            if validation.rejected:
                                self._write_quarantine(
                                    validation.rejected,
                                    metadata,
                                    ingestion_date,
                                    part_number,
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
                            part_number += 2

                    status = "completed" if counters["ingested"] else "empty"
                    self._refresh_layers(connection)
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

    @staticmethod
    def _finish_batch(
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
            return rows
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
            full_rows.append(employee)
        return full_rows

    def _apply_records(self, connection, rows: list[dict[str, Any]], source_type: SourceType) -> None:
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
            connection.execute(
                f"INSERT OR REPLACE INTO {table} ({selected_columns}) "
                f"SELECT {selected_columns} FROM {relation_name}"
            )
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

    def _refresh_layers(self, connection) -> None:
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