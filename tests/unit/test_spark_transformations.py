"""Spark SQL transformation tests; execute with the optional phase3 extra."""

from __future__ import annotations

import pytest
import os
import sys
from pyspark.sql import functions as F

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from streamlake.processing.spark.employees import clean_employee, employee_change_history, process_incremental
from streamlake.processing.spark.entities import validate_dependents


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = SparkSession.builder.master("local[2]").appName("streamlake-phase3-tests").getOrCreate()
    yield session
    session.stop()


def _employees(spark):
    return spark.createDataFrame([
        ("E1", "Ada", "ADA@EXAMPLE.TEST", "Female", "1990-01-01", "2020-01-01", "C1", "North", "Eng", "Engineer", 500000, "P1", "Self", "active", "2020-01-01", "2020-01-01", "b1", "a.csv", "2020-01-01"),
        ("E1", "Ada L", "ada@example.test", "Female", "1990-01-01", "2020-01-01", "C1", "North", "Eng", "Lead", 750000, "P1", "Self", "active", "2020-01-01", "2021-01-01", "b2", "u.csv", "2021-01-01"),
        ("E2", "Bad", "bad-email", "X", "1990-01-01", "2022-01-01", "C1", "North", "Eng", "Dev", 100000, "P1", "Self", "active", "2022-01-01", "2022-01-01", "b3", "x.csv", "2022-01-01"),
    ], "employee_id string, employee_name string, email string, gender string, date_of_birth string, joining_date string, company_id string, company_name string, department string, designation string, sum_insured long, policy_id string, relationship string, record_status string, created_at string, updated_at string, source_batch_id string, source_file string, source_event_timestamp string")


def test_cleaning_invalid_quarantine_dedup_and_incremental(spark):
    cleaned = clean_employee(_employees(spark))
    assert cleaned.where("not is_valid").count() == 1
    latest = process_incremental(cleaned.where("is_valid"))
    assert latest.count() == 1
    assert latest.first().employee_name == "Ada L"


def test_employee_history_has_one_current_scd2_version(spark):
    cleaned = clean_employee(_employees(spark).where("employee_id = 'E1'"))
    history = employee_change_history(cleaned)
    assert history.count() == 2
    assert history.where("is_current").count() == 1
    assert history.where("effective_end_date is not null").count() == 1


def test_delete_event_wins_when_source_timestamps_tie(spark):
    seed = _employees(spark).where("employee_id = 'E1'").limit(1)
    common = seed.withColumn("source_event_timestamp", F.to_timestamp(F.lit("2024-01-01 00:00:00")))
    insert = common.withColumn("source_batch_id", F.lit("a")).withColumn("source_operation", F.lit("c"))
    update = common.withColumn("source_batch_id", F.lit("b")).withColumn("source_operation", F.lit("u"))
    delete = common.withColumn("source_batch_id", F.lit("c")).withColumn("source_operation", F.lit("d")).withColumn("record_status", F.lit("deleted"))
    history = employee_change_history(clean_employee(insert.unionByName(update).unionByName(delete)))
    current = history.where("is_current").first()
    assert current.source_operation == "d"
    assert current.record_status == "deleted"


def test_dependent_referential_integrity(spark):
    employees = spark.createDataFrame([("E1",)], "employee_id string")
    dependents = spark.createDataFrame([("E1", "D1", "Child", "2000-01-01"), ("E9", "D2", "Orphan", "2000-01-01")], "employee_id string, dependent_id string, dependent_name string, date_of_birth string")
    accepted, rejected = validate_dependents(dependents, employees)
    assert accepted.count() == 1
    assert rejected.first().quality_errors == ["unknown_employee"]
