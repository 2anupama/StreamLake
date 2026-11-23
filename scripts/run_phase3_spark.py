"""Run the local Bronze-to-Silver Spark transformations and quality checks."""

from __future__ import annotations

import logging
import hashlib
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, Window

from streamlake.processing.spark.employees import EMPLOYEE_COLUMNS, clean_employee, employee_change_history
from streamlake.processing.spark.entities import aggregate_companies, transform_policies, validate_dependents
from streamlake.quality.spark import quality_report, write_quality_report


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = DATA / "phase3"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("streamlake.phase3")


def _source(spark: SparkSession, filename: str, source_type: str):
    path = DATA / "incoming" / filename
    batch_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    frame = spark.read.option("header", True).option("inferSchema", False).csv(str(path))
    has_updated_at = "updated_at" in frame.columns
    return (
        frame
        .withColumn("source_file", F.lit(filename))
        .withColumn("source_batch_id", F.lit(f"{source_type}-{batch_digest}"))
        .withColumn("source_event_timestamp", F.expr("try_cast(updated_at as timestamp)") if has_updated_at else F.current_timestamp())
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )


def _write(frame, path: Path, *, partition: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = frame.coalesce(4)
    writer = output.write.mode("overwrite")
    if partition and partition in frame.columns:
        writer = writer.partitionBy(partition)
    writer.parquet(str(path))


def main() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", os.sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", os.sys.executable)
    spark = SparkSession.builder.appName("StreamLake-Phase3-Transform").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    additions = _source(spark, "employees_additions.csv", "additions")
    updates = _source(spark, "employees_updates.csv", "updates")
    deletes = _source(spark, "employees_deletions.csv", "deletions")
    dependents = _source(spark, "employee_dependents.csv", "dependents")

    # Preserve update ordering and source lineage. Deletion rows use the last
    # known employee attributes while retaining the business delete status.
    additions = additions.withColumn("source_operation", F.lit("c"))
    updates = updates.withColumn("source_operation", F.lit("u"))
    base = additions.unionByName(updates, allowMissingColumns=True)
    base = base.withColumn("record_status", F.coalesce("record_status", F.lit("active")))
    base_order = Window.partitionBy("employee_id").orderBy(F.col("source_event_timestamp").asc_nulls_first(), F.col("source_batch_id"))
    deletion_cols = [F.col(c) if c in deletes.columns else F.lit(None).cast("string").alias(c) for c in EMPLOYEE_COLUMNS]
    deletion_rows = deletes.select(*deletion_cols, "source_file", "source_batch_id", "source_event_timestamp", "ingestion_timestamp")
    deletion_rows = deletion_rows.withColumn("record_status", F.lit("deleted")).withColumn("source_operation", F.lit("d"))

    cdc_events = None
    cdc_root = DATA / "bronze" / "cdc_v2"
    if any(cdc_root.rglob("*.parquet")):
        raw_cdc = spark.read.option("basePath", str(cdc_root)).parquet(str(cdc_root))
        raw_cdc = raw_cdc.where(
            (F.col("source_table") == "employees") & F.col("error_reason").isNull()
            & F.col("operation").isin("c", "u", "d") & ~F.coalesce(F.col("is_tombstone"), F.lit(False))
        )
        payload_json = F.coalesce("after_json", "before_json")
        cdc_fields = [
            F.col("employee_id") if column == "employee_id" else
            F.when(F.col("operation") == "d", F.lit("deleted"))
              .otherwise(F.get_json_object(payload_json, f"$.{column}")).alias(column) if column == "record_status" else
            F.get_json_object(payload_json, f"$.{column}").alias(column)
            for column in EMPLOYEE_COLUMNS
        ]
        cdc_events = raw_cdc.select(
            *cdc_fields,
            F.concat(F.lit("kafka:"), F.col("topic")).alias("source_file"),
            F.concat(F.lit("cdc:"), F.col("event_id")).alias("source_batch_id"),
            F.coalesce(
                F.timestamp_seconds((F.col("event_timestamp_ms") / 1000).cast("long")),
                F.col("kafka_timestamp"),
            ).alias("source_event_timestamp"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.col("operation").alias("source_operation"),
            F.col("event_id").alias("source_event_id"),
            "before_json", "after_json", "source_lsn", "topic", "partition", "offset",
        )

    batch_rows = base.select(
        *(EMPLOYEE_COLUMNS + ("source_file", "source_batch_id", "source_event_timestamp", "ingestion_timestamp", "source_operation"))
    ).unionByName(deletion_rows, allowMissingColumns=True)
    batch_rows = batch_rows.withColumn(
        "source_event_id",
        F.sha2(F.to_json(F.struct("source_file", "employee_id", "source_event_timestamp", *[F.col(c) for c in EMPLOYEE_COLUMNS])), 256),
    )
    all_rows = batch_rows.unionByName(cdc_events, allowMissingColumns=True) if cdc_events is not None else batch_rows
    all_rows = all_rows.withColumn("_event_order", F.row_number().over(base_order))
    carry_window = Window.partitionBy("employee_id").orderBy(F.col("source_event_timestamp").asc_nulls_first(), F.col("source_batch_id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    for field in EMPLOYEE_COLUMNS:
        if field not in {"employee_id", "record_status", "updated_at"}:
            all_rows = all_rows.withColumn(field, F.last(F.col(field), ignorenulls=True).over(carry_window))
    all_rows = all_rows.withColumn("record_status", F.coalesce("record_status", F.lit("active")))
    cleaned = clean_employee(all_rows)
    # Business deletes have all required prior attributes after carry-forward.
    valid_rows = cleaned.where(F.col("is_valid"))
    invalid = cleaned.where(~F.col("is_valid"))
    latest_priority = F.when(F.col("source_operation") == "d", 3).when(F.col("source_operation") == "u", 2).otherwise(1)
    latest_window = Window.partitionBy("employee_id").orderBy(F.col("source_event_timestamp").desc_nulls_last(), latest_priority.desc(), F.col("source_event_id").desc())
    current = valid_rows.withColumn("_rank", F.row_number().over(latest_window)).where("_rank = 1").drop("_rank")
    history = employee_change_history(valid_rows)

    valid_dependents, invalid_dependents = validate_dependents(dependents.withColumn("date_of_birth", F.expr("try_cast(date_of_birth as date)")), valid_rows.where(F.col("record_status") != "deleted"))
    policies = transform_policies(current.where(F.col("record_status") != "deleted"))
    companies = aggregate_companies(current)
    changes = all_rows.select(
        F.coalesce("source_event_id", F.sha2(F.to_json(F.struct("source_batch_id", "employee_id", "source_event_timestamp", "source_file", "record_status")), 256)).alias("event_id"),
        F.col("employee_id"), F.col("source_operation").alias("operation"),
        F.col("source_event_timestamp").alias("event_timestamp"), "source_file", "source_batch_id", "record_status",
        "before_json", "after_json", "source_lsn", "topic", "partition", "offset",
    ).dropDuplicates(["event_id"])
    quality, invalid_quality = quality_report(current, required=("employee_id", "company_id", "policy_id"), unique_keys=("employee_id",))
    quality["invalid_employee_rows"] = invalid.count()
    quality["invalid_dependent_rows"] = invalid_dependents.count()
    quality["quality_check_rows"] = invalid_quality.count()
    employee_source_rows = all_rows.count()
    employee_valid_rows = valid_rows.count()
    employee_rejected_rows = invalid.count()
    employee_deduplicated_rows = employee_valid_rows - history.count()
    quality.update({
        "employee_source_rows": employee_source_rows,
        "employee_valid_event_rows": employee_valid_rows,
        "employee_rejected_rows": employee_rejected_rows,
        "employee_duplicate_rows_removed": employee_deduplicated_rows,
        "employee_source_target_reconciled": employee_source_rows == history.count() + employee_rejected_rows + employee_deduplicated_rows,
        "dependent_source_rows": dependents.count(),
        "dependent_accepted_rows": valid_dependents.count(),
        "dependent_rejected_rows": invalid_dependents.count(),
        "dependent_source_target_reconciled": dependents.count() == valid_dependents.count() + invalid_dependents.count(),
    })

    for name, frame in {
        "employee_current": current, "employee_history": history,
        "dependents": valid_dependents, "policies": policies,
        "companies": companies, "employee_changes": changes,
    }.items():
        _write(frame, OUT / "silver" / name)
    _write(invalid, OUT / "quarantine" / "employees")
    _write(invalid_dependents, OUT / "quarantine" / "dependents")
    write_quality_report(quality, OUT / "quality" / "quality_report.json")
    logger.info("Phase 3 Spark outputs ready: current=%d history=%d dependents=%d", current.count(), history.count(), valid_dependents.count())
    spark.stop()


if __name__ == "__main__":
    main()
