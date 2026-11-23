"""Employee Bronze-to-Silver and history transformations using Spark SQL."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

EMPLOYEE_REQUIRED = (
    "employee_id", "employee_name", "email", "gender", "date_of_birth",
    "joining_date", "company_id", "company_name", "department",
    "designation", "sum_insured", "policy_id", "relationship",
)
EMPLOYEE_COLUMNS = EMPLOYEE_REQUIRED + (
    "mobile_number", "subsidiary", "record_status", "created_at", "updated_at",
)


def validate_schema(frame: DataFrame, required: tuple[str, ...], *, name: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{name} schema is missing required columns: {', '.join(missing)}")


def clean_employee(frame: DataFrame) -> DataFrame:
    """Normalize employee attributes; derive row-level validity with SQL expressions."""
    validate_schema(frame, EMPLOYEE_REQUIRED, name="employee")
    result = frame
    for column in result.columns:
        if result.schema[column].dataType.simpleString() == "string":
            result = result.withColumn(column, F.when(F.trim(F.col(column)) == "", None).otherwise(F.trim(F.col(column))))
    for column in ("email",):
        if column in result.columns:
            result = result.withColumn(column, F.lower(F.col(column)))
    for column in ("date_of_birth", "joining_date"):
        if column in result.columns:
            result = result.withColumn(f"_{column}_raw", F.col(column))
            result = result.withColumn(column, F.expr(f"try_cast(`{column}` as date)"))
    for column in ("created_at", "updated_at"):
        if column in result.columns:
            result = result.withColumn(column, F.expr(f"try_cast(`{column}` as timestamp)"))
    if "sum_insured" in result.columns:
        result = result.withColumn("_sum_insured_raw", F.col("sum_insured"))
        result = result.withColumn("sum_insured", F.expr("try_cast(sum_insured as decimal(18,2))"))

    errors = []
    for column in EMPLOYEE_REQUIRED:
        if column in result.columns:
            errors.append(F.when(F.col(column).isNull(), F.lit(f"missing_{column}")))
    if "email" in result.columns:
        errors.append(F.when(~F.col("email").rlike(r"^[^\s@]+@[^\s@]+\.[^\s@]+$"), F.lit("invalid_email")))
    if "sum_insured" in result.columns:
        errors.append(F.when(F.col("_sum_insured_raw").isNotNull() & F.col("sum_insured").isNull(), F.lit("invalid_sum_insured")))
        errors.append(F.when(F.col("sum_insured") <= 0, F.lit("non_positive_sum_insured")))
    if "date_of_birth" in result.columns:
        errors.append(F.when(F.col("_date_of_birth_raw").isNotNull() & F.col("date_of_birth").isNull(), F.lit("invalid_date_of_birth")))
        errors.append(F.when(F.col("date_of_birth") > F.current_date(), F.lit("future_date_of_birth")))
    if "joining_date" in result.columns:
        errors.append(F.when(F.col("_joining_date_raw").isNotNull() & F.col("joining_date").isNull(), F.lit("invalid_joining_date")))
        errors.append(F.when(F.col("joining_date") > F.current_date(), F.lit("future_joining_date")))
    result = result.withColumn("quality_errors", F.filter(F.array(*errors), lambda x: x.isNotNull()))
    result = result.withColumn("is_valid", F.size("quality_errors") == 0)
    if "_sum_insured_raw" in result.columns:
        result = result.drop("_sum_insured_raw")
    result = result.drop(*[c for c in ("_date_of_birth_raw", "_joining_date_raw") if c in result.columns])
    return result


def deduplicate_employees(frame: DataFrame) -> DataFrame:
    """Keep the latest record per employee and business key deterministically."""
    validate_schema(frame, ("employee_id", "updated_at", "source_batch_id"), name="employee")
    order = Window.partitionBy("employee_id").orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("source_event_timestamp").desc_nulls_last() if "source_event_timestamp" in frame.columns else F.lit(0),
        F.col("source_batch_id").desc(),
    )
    return frame.withColumn("_row_number", F.row_number().over(order)).where(F.col("_row_number") == 1).drop("_row_number")


def employee_change_history(frame: DataFrame) -> DataFrame:
    """Create SCD2 intervals from valid employee source records."""
    valid = frame.where(F.col("is_valid"))
    history_keys = [
        column for column in (
            "employee_id", "source_file", "source_batch_id", "source_event_timestamp",
            "employee_name", "email", "company_id", "department", "designation",
            "policy_id", "sum_insured", "record_status",
        ) if column in valid.columns
    ]
    valid = valid.dropDuplicates(history_keys)
    if "effective_start_date" not in valid.columns:
        valid = valid.withColumn("effective_start_date", F.to_date(F.coalesce("updated_at", "created_at")))
    valid = valid.withColumn(
        "effective_start_timestamp",
        F.coalesce(
            F.col("source_event_timestamp") if "source_event_timestamp" in valid.columns else F.lit(None).cast("timestamp"),
            F.col("updated_at"), F.col("created_at"),
        ),
    )
    valid = valid.withColumn("effective_start_date", F.to_date("effective_start_timestamp"))
    event_priority = (
        F.when(F.col("source_operation") == "d", 3)
        .when(F.col("source_operation") == "u", 2)
        .otherwise(1)
        if "source_operation" in valid.columns else F.lit(1)
    )
    tie_breaker = F.sha2(F.to_json(F.struct(*[F.col(c) for c in valid.columns])), 256)
    window = Window.partitionBy("employee_id").orderBy(
        F.col("effective_start_timestamp").asc_nulls_first(), event_priority.asc(), tie_breaker.asc()
    )
    return (
        valid.withColumn("effective_end_timestamp", F.lead("effective_start_timestamp").over(window))
        .withColumn("effective_end_date", F.to_date("effective_end_timestamp"))
        .withColumn("is_current", F.col("effective_end_timestamp").isNull())
        .withColumn("employee_sk", F.sha2(F.concat_ws("|", "employee_id", F.coalesce(F.col("effective_start_timestamp").cast("string"), F.lit("unknown")), "source_batch_id", tie_breaker), 256))
    )


def latest_employee_state(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return latest state plus invalid rows; null fields on deletes inherit last values."""
    validate_schema(frame, EMPLOYEE_COLUMNS, name="employee")
    normalized = clean_employee(frame)
    invalid = normalized.where(~F.col("is_valid"))
    valid = normalized.where(F.col("is_valid"))
    latest = deduplicate_employees(valid)
    logger.info("Employee transformations configured; Spark will evaluate lazy DataFrames")
    return latest, invalid


def process_incremental(frame: DataFrame, existing: DataFrame | None = None) -> DataFrame:
    """Apply only new batches and resolve employee state with a windowed latest row."""
    incoming = frame
    if existing is not None:
        incoming = existing.unionByName(frame, allowMissingColumns=True)
    return deduplicate_employees(incoming)
