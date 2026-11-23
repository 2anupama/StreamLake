"""Reusable Spark quality checks with row quarantine and reconciliation reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def quality_report(frame: DataFrame, *, required: Iterable[str], unique_keys: Iterable[str]) -> tuple[dict, DataFrame]:
    errors = []
    for name in required:
        errors.append(F.when(F.col(name).isNull(), F.lit(f"missing_{name}")))
    duplicate_window = __import__("pyspark.sql", fromlist=["Window"]).Window.partitionBy(*unique_keys)
    inspected = frame.withColumn("_duplicate_count", F.count(F.lit(1)).over(duplicate_window))
    errors.append(F.when(F.col("_duplicate_count") > 1, F.lit("duplicate_business_key")))
    inspected = inspected.withColumn("quality_errors", F.filter(F.array(*errors), lambda x: x.isNotNull()))
    failed = inspected.where(F.size("quality_errors") > 0).drop("_duplicate_count")
    report = {
        "input_rows": frame.count(),
        "accepted_rows": inspected.where(F.size("quality_errors") == 0).count(),
        "rejected_rows": failed.count(),
        "required_columns": list(required),
        "unique_keys": list(unique_keys),
    }
    report["source_target_reconciled"] = report["input_rows"] == report["accepted_rows"] + report["rejected_rows"]
    return report, failed


def write_quality_report(report: dict, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
