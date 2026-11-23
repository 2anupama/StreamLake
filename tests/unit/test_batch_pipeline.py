"""Tests for the local batch ingestion pipeline and medallion outputs."""

import json

import pandas as pd
import pytest
from openpyxl import Workbook

from streamlake.common.settings import Settings
from streamlake.ingestion.batch.generator import generate_synthetic_data
from streamlake.ingestion.batch.pipeline import BatchPipeline
from streamlake.ingestion.batch.reader import iter_input_chunks, read_column_names
from streamlake.ingestion.batch.schemas import EMPLOYEE_COLUMNS, SourceType
from streamlake.ingestion.batch.validation import validate_chunk


def make_settings(tmp_path, chunk_size: int = 7) -> Settings:
    data_root = tmp_path / "data"
    return Settings(
        data_root=data_root,
        duckdb_path=data_root / "streamlake.duckdb",
        ingestion_audit_path=tmp_path / "logs" / "ingestion_audit.jsonl",
        batch_chunk_size=chunk_size,
    )


def test_generator_creates_at_least_ten_thousand_employee_records(tmp_path) -> None:
    counts = generate_synthetic_data(tmp_path, employee_count=10_000, seed=3)

    additions = pd.read_csv(tmp_path / "employees_additions.csv", dtype="string")
    assert len(additions) >= 10_000
    assert len(additions.columns) == len(EMPLOYEE_COLUMNS)
    assert counts["employees_updates.csv"] > 0
    assert counts["employees_deletions.csv"] > 0
    assert counts["employee_dependents.csv"] > 0
    assert additions["employee_id"].duplicated().any()
    assert (additions["email"].str.strip() == "").any()
    assert "not-a-number" in set(additions["sum_insured"])


def test_reader_chunks_csv_and_reads_excel_headers(tmp_path) -> None:
    csv_path = tmp_path / "records_additions.csv"
    pd.DataFrame({" Employee ID ": ["E1", "E2", "E3"], "Email": ["a@x.test"] * 3}).to_csv(
        csv_path, index=False
    )

    csv_chunks = list(iter_input_chunks(csv_path, chunk_size=2))
    assert [len(chunk.frame) for chunk in csv_chunks] == [2, 1]
    assert read_column_names(csv_path) == ["employee_id", "email"]

    excel_path = tmp_path / "records_updates.xlsx"
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(["Employee ID", "Email"])
    worksheet.append(["E1", "a@x.test"])
    worksheet.append(["E2", "b@x.test"])
    workbook.save(excel_path)
    assert read_column_names(excel_path) == ["employee_id", "email"]
    assert sum(len(chunk.frame) for chunk in iter_input_chunks(excel_path, 1)) == 2


def test_validation_normalizes_and_quarantines_bad_employee_rows() -> None:
    row = dict(
        zip(
            EMPLOYEE_COLUMNS,
            [
                "EMP-1", " Jane Doe ", "  Jane@Example.COM ", "(415) 555-0101", "female",
                "1980-01-02", "2020-03-04", "c001", "Example Co", "West", "HR", "Analyst",
                "500000", "p-1", "Self", "active", "2024-01-01T10:00:00Z", "2024-01-02T10:00:00Z",
            ],
        )
    )
    missing_email = dict(row, employee_id="EMP-2", email=" ")
    invalid_date = dict(row, employee_id="EMP-3", date_of_birth="2023-02-30")
    invalid_coverage = dict(row, employee_id="EMP-4", sum_insured="unlimited")
    duplicate = dict(row)

    result = validate_chunk(
        pd.DataFrame([row, duplicate, missing_email, invalid_date, invalid_coverage]),
        SourceType.ADDITIONS,
    )

    assert len(result.accepted) == 1
    assert result.accepted[0]["employee_name"] == "Jane Doe"
    assert result.accepted[0]["email"] == "jane@example.com"
    assert result.accepted[0]["mobile_number"] == "+14155550101"
    assert result.accepted[0]["date_of_birth"] == "1980-01-02"
    reasons = " ".join(rejection["error_reason"] for rejection in result.rejected)
    assert "Duplicate employee_id" in reasons
    assert "Missing required field: email" in reasons
    assert "Invalid date_of_birth" in reasons
    assert "Invalid sum_insured" in reasons
    assert json.loads(result.rejected[0]["original_record"])["employee_id"] == "EMP-1"


def test_pipeline_writes_bronze_silver_gold_and_quarantine(tmp_path) -> None:
    settings = make_settings(tmp_path)
    incoming = settings.data_root / "incoming"
    generate_synthetic_data(incoming, employee_count=40, seed=11)
    additions_path = incoming / "employees_additions.csv"
    additions = pd.read_csv(additions_path, dtype="string", keep_default_na=False)
    additions.loc[3, "employee_name"] = "  Original Name  "
    additions.loc[3, "email"] = "  ORIGINAL@EXAMPLE.COM  "
    additions.to_csv(additions_path, index=False)
    deletions_path = incoming / "employees_deletions.csv"
    deletions = pd.read_csv(deletions_path, dtype="string", keep_default_na=False)
    deletions.loc[0, "employee_id"] = "EMP-0000001"
    deletions.to_csv(deletions_path, index=False)

    pipeline = BatchPipeline(settings)
    results = pipeline.run_batch()

    assert [result.source_type for result in results] == [
        "additions", "updates", "dependents", "deletions"
    ]
    assert results[0].rows_ingested == 45
    assert results[0].rows_loaded == 40
    assert results[0].rows_rejected == 5
    assert sum(result.rows_loaded for result in results) > 40
    bronze_path = next(
        (settings.data_root / "bronze").glob("**/source_type=additions/**/*.parquet")
    )
    bronze_files = list(
        (settings.data_root / "bronze").glob("**/source_type=additions/**/*.parquet")
    )
    bronze = pd.concat([pd.read_parquet(path) for path in bronze_files], ignore_index=True)
    assert {"ingestion_timestamp", "batch_id", "source_file", "source_system", "ingestion_status"} <= set(bronze.columns)
    raw_employee = bronze.loc[bronze["employee_id"] == "EMP-0000004"].iloc[0]
    assert raw_employee["employee_name"] == "  Original Name  "
    assert raw_employee["email"] == "  ORIGINAL@EXAMPLE.COM  "

    silver = pd.read_parquet(settings.data_root / "silver" / "silver_employee_master.parquet")
    employee = silver.loc[silver["employee_id"] == "EMP-0000004"].iloc[0]
    assert employee["employee_name"] == "Original Name"
    assert employee["email"] == "original@example.com"
    addition_events = pd.concat(
        [
            pd.read_parquet(path)
            for path in (settings.data_root / "silver" / "events").glob(
                "**/source_type=additions/**/*.parquet"
            )
        ],
        ignore_index=True,
    )
    assert "Original Name" in set(addition_events["employee_name"])

    assert len(pd.read_parquet(settings.data_root / "gold" / "gold_employee_master.parquet")) == 39
    assert len(pd.read_parquet(settings.data_root / "gold" / "gold_employee_dependents.parquet")) == 10
    assert pipeline.report()["active_dependents"] == 10
    assert len(pd.read_parquet(settings.data_root / "gold" / "gold_company_employee_summary.parquet")) > 0
    assert len(pd.read_parquet(settings.data_root / "gold" / "gold_policy_summary.parquet")) > 0
    assert sum(len(pd.read_parquet(path)) for path in (settings.data_root / "quarantine").glob("**/*.parquet")) >= 7
    assert settings.ingestion_audit_path.exists()


def test_validate_command_reports_row_level_rejections(tmp_path) -> None:
    settings = make_settings(tmp_path)
    incoming = settings.data_root / "incoming"
    generate_synthetic_data(incoming, employee_count=20, seed=13)
    (incoming / ".gitkeep").touch()
    pipeline = BatchPipeline(settings)

    outcomes = pipeline.validate_inputs()

    assert ".gitkeep" not in {item["file"] for item in outcomes}
    additions = next(item for item in outcomes if item["source_type"] == "additions")
    dependents = next(item for item in outcomes if item["source_type"] == "dependents")
    assert additions["status"] == "invalid"
    assert additions["valid_rows"] == 20
    assert additions["rejected_rows"] == 5
    assert dependents["rejected_rows"] == 2


def test_pipeline_is_idempotent_for_same_file(tmp_path) -> None:
    settings = make_settings(tmp_path)
    incoming = settings.data_root / "incoming"
    generate_synthetic_data(incoming, employee_count=10, seed=5)
    pipeline = BatchPipeline(settings)
    source = incoming / "employees_additions.csv"

    first = pipeline.ingest_file(source)
    replay = pipeline.ingest_file(source)

    assert first.rows_loaded == 10
    assert replay.skipped
    assert replay.duplicate_of == first.batch_id
    report = pipeline.report()
    assert report["active_employees"] == 10
    connection = pipeline._connect()
    try:
        history_count = connection.execute("SELECT count(*) FROM batch_history").fetchone()[0]
    finally:
        connection.close()
    assert history_count == 2


def test_malformed_csv_records_are_quarantined(tmp_path) -> None:
    settings = make_settings(tmp_path)
    source = settings.data_root / "incoming" / "employees_additions.csv"
    source.parent.mkdir(parents=True)
    source.write_text(
        ",".join(EMPLOYEE_COLUMNS)
        + "\n"
        + "E1,Name,e@example.test,4155550101,Female,1980-01-01,2020-01-01,C1,Co,West,HR,Analyst,500000,P1,Self,active,2024-01-01,2024-01-01\n"
        + ",".join(["E2"] + ["extra"] * 20)
        + "\n",
        encoding="utf-8",
    )
    pipeline = BatchPipeline(settings)

    result = pipeline.ingest_file(source)

    assert result.rows_ingested == 2
    assert result.rows_processed == 1
    assert result.rows_rejected == 1
    quarantine_path = next((settings.data_root / "quarantine").glob("**/*.parquet"))
    quarantine = pd.read_parquet(quarantine_path)
    assert "Malformed CSV row" in quarantine.iloc[0]["error_reason"]


def test_empty_file_and_failed_schema_are_audited(tmp_path) -> None:
    settings = make_settings(tmp_path)
    incoming = settings.data_root / "incoming"
    incoming.mkdir(parents=True)
    empty = incoming / "empty_additions.csv"
    empty.write_text("  \n", encoding="utf-8")
    pipeline = BatchPipeline(settings)

    assert pipeline.ingest_file(empty).status == "empty"

    invalid = incoming / "bad_additions.csv"
    invalid.write_text("employee_id,email\nE1,e@example.test\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Missing required columns"):
        pipeline.ingest_file(invalid)
    audit = settings.ingestion_audit_path.read_text(encoding="utf-8")
    assert '"status": "empty"' in audit
    assert '"status": "failed"' in audit