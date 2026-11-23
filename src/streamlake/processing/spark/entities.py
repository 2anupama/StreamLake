"""Dependent, policy, and company transformations."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from streamlake.processing.spark.employees import validate_schema


def validate_dependents(frame: DataFrame, employees: DataFrame) -> tuple[DataFrame, DataFrame]:
    validate_schema(frame, ("employee_id", "dependent_id", "dependent_name", "date_of_birth"), name="dependent")
    duplicate_window = Window.partitionBy("dependent_id")
    frame = frame.withColumn("_dependent_duplicate_count", F.count(F.lit(1)).over(duplicate_window))
    joined = frame.join(employees.select("employee_id").distinct().withColumn("_employee_exists", F.lit(True)), "employee_id", "left")
    checked = joined.withColumn(
        "quality_errors",
        F.filter(F.array(
            F.when(F.col("dependent_id").isNull(), "missing_dependent_id"),
            F.when(F.col("dependent_name").isNull(), "missing_dependent_name"),
            F.when(F.col("_employee_exists").isNull(), "unknown_employee"),
            F.when(F.expr("try_cast(date_of_birth as date)").isNull(), "invalid_date_of_birth"),
            F.when(F.col("_dependent_duplicate_count") > 1, "duplicate_dependent_id"),
        ), lambda x: x.isNotNull()),
    ).drop("_employee_exists", "_dependent_duplicate_count")
    return checked.where(F.size("quality_errors") == 0), checked.where(F.size("quality_errors") > 0)


def transform_policies(employees: DataFrame) -> DataFrame:
    validate_schema(employees, ("policy_id", "sum_insured"), name="policy source")
    return employees.groupBy("policy_id", "company_id").agg(
        F.max("company_name").alias("company_name"),
        F.countDistinct("employee_id").alias("enrolled_employee_count"),
        F.sum(F.col("sum_insured").cast("decimal(18,2)")).alias("total_sum_insured"),
        F.max("updated_at").alias("source_updated_at"),
    )


def aggregate_companies(employees: DataFrame) -> DataFrame:
    return employees.groupBy("company_id").agg(
        F.max("company_name").alias("company_name"),
        F.countDistinct(F.when(F.col("record_status") != "deleted", F.col("employee_id"))).alias("active_employee_count"),
        F.countDistinct("policy_id").alias("policy_count"),
        F.sum(F.when(F.col("record_status") != "deleted", F.col("sum_insured")).otherwise(F.lit(0))).alias("total_sum_insured"),
    )


def monthly_employee_changes(events: DataFrame) -> DataFrame:
    """Aggregate operation events without discarding source event metadata."""
    return events.groupBy(F.date_trunc("month", "event_timestamp").alias("change_month"), "operation").agg(
        F.countDistinct("event_id").alias("event_count"), F.countDistinct("employee_id").alias("employee_count")
    )
