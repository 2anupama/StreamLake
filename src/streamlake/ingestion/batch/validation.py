"""Row-level cleaning, standardization, and validation rules."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd

from streamlake.ingestion.batch.schemas import SourceType

EMAIL_PATTERN = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
EMPLOYEE_REQUIRED_FIELDS = (
    "employee_id",
    "employee_name",
    "email",
    "gender",
    "date_of_birth",
    "joining_date",
    "company_id",
    "company_name",
    "department",
    "designation",
    "sum_insured",
    "policy_id",
    "relationship",
    "record_status",
)


@dataclass
class ValidationResult:
    accepted: list[dict[str, Any]]
    rejected: list[dict[str, str]]


def clean_value(value: Any) -> Any:
    """Trim text and map pandas nulls or blank text to Python ``None``."""
    if value is None or value is pd.NA:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def standardize_phone(value: Any) -> str | None:
    """Normalize common US and international phone representations."""
    value = clean_value(value)
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if 7 <= len(digits) <= 15:
        return f"+{digits}"
    return None


def _standardize_date(value: Any) -> str | None:
    value = clean_value(value)
    if value is None:
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce", format="mixed")
    except (TypeError, ValueError, OverflowError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _standardize_timestamp(value: Any) -> str | None:
    value = clean_value(value)
    if value is None:
        return None
    try:
        parsed = pd.to_datetime(value, errors="coerce", format="mixed")
    except (TypeError, ValueError, OverflowError):
        return None
    if pd.isna(parsed):
        return None
    return parsed.isoformat(timespec="seconds")


def _coverage_amount(value: Any) -> int | None:
    value = clean_value(value)
    if value is None:
        return None
    try:
        amount = Decimal(str(value).replace(",", ""))
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite() or amount <= 0 or amount != amount.to_integral_value():
        return None
    return int(amount)


def validate_chunk(
    frame: pd.DataFrame,
    source_type: SourceType,
    *,
    all_employee_ids: set[str] | None = None,
    active_employee_ids: set[str] | None = None,
    existing_dependent_ids: set[str] | None = None,
    seen_ids: set[str] | None = None,
) -> ValidationResult:
    """Clean one input chunk and return accepted rows and rejection details.

    The caller supplies reference IDs from DuckDB and retains ``seen_ids`` across
    chunks so duplicate detection is bounded to one key set rather than a full
    dataframe in memory.
    """
    all_employee_ids = all_employee_ids or set()
    active_employee_ids = active_employee_ids or set()
    existing_dependent_ids = existing_dependent_ids or set()
    seen_ids = seen_ids if seen_ids is not None else set()
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []

    for source_record in frame.to_dict(orient="records"):
        record = {str(key): clean_value(value) for key, value in source_record.items()}
        if source_type is SourceType.DELETIONS:
            employee_id = record.get("employee_id")
            errors: list[str] = []
            if not employee_id:
                errors.append("Missing employee_id")
            elif employee_id in seen_ids:
                errors.append("Duplicate employee_id in source file")
            elif employee_id not in active_employee_ids:
                errors.append("Employee does not exist or is already deleted")
            updated_at = _standardize_timestamp(record.get("updated_at"))
            if record.get("updated_at") and updated_at is None:
                errors.append("Invalid updated_at")
            if errors:
                rejected.append(_rejection(source_record, errors))
                continue
            seen_ids.add(employee_id)
            accepted.append(
                {
                    "employee_id": employee_id,
                    "record_status": "deleted",
                    "updated_at": updated_at,
                }
            )
            continue

        if source_type in (SourceType.ADDITIONS, SourceType.UPDATES):
            employee_id = record.get("employee_id")
            errors = []
            if not employee_id:
                errors.append("Missing employee_id")
            elif employee_id in seen_ids:
                errors.append("Duplicate employee_id in source file")
            elif source_type is SourceType.ADDITIONS and employee_id in all_employee_ids:
                errors.append("Employee ID already exists")
            elif source_type is SourceType.UPDATES and employee_id not in active_employee_ids:
                errors.append("Employee update references an unknown or deleted employee")

            for field in EMPLOYEE_REQUIRED_FIELDS:
                if not record.get(field):
                    errors.append(f"Missing required field: {field}")

            email = record.get("email")
            if email:
                email = str(email).lower()
                record["email"] = email
                if not EMAIL_PATTERN.fullmatch(email):
                    errors.append("Invalid email format")
            if email and not EMAIL_PATTERN.fullmatch(str(email)):
                pass

            for field in ("date_of_birth", "joining_date"):
                original_value = record.get(field)
                standardized = _standardize_date(original_value)
                record[field] = standardized
                if original_value and standardized is None:
                    errors.append(f"Invalid {field}")
            if record.get("created_at"):
                standardized = _standardize_timestamp(record["created_at"])
                if standardized is None:
                    errors.append("Invalid created_at")
                record["created_at"] = standardized
            if record.get("updated_at"):
                standardized = _standardize_timestamp(record["updated_at"])
                if standardized is None:
                    errors.append("Invalid updated_at")
                record["updated_at"] = standardized

            coverage = _coverage_amount(record.get("sum_insured"))
            if coverage is None:
                errors.append("Invalid sum_insured; expected a positive whole number")
            record["sum_insured"] = coverage
            if record.get("mobile_number"):
                record["mobile_number"] = standardize_phone(record["mobile_number"])
            if record.get("gender"):
                record["gender"] = str(record["gender"]).title()
            for field in ("company_id", "policy_id"):
                if record.get(field):
                    record[field] = str(record[field]).upper()
            if record.get("record_status") and str(record["record_status"]).lower() != "active":
                errors.append("Employee record_status must be active")
            else:
                record["record_status"] = "active"

            if errors:
                rejected.append(_rejection(source_record, errors))
                continue
            seen_ids.add(str(employee_id))
            accepted.append(record)
            continue

        dependent_id = record.get("dependent_id")
        employee_id = record.get("employee_id")
        errors = []
        if not employee_id:
            errors.append("Missing employee_id")
        elif employee_id not in active_employee_ids:
            errors.append("Dependent references an unknown or deleted employee")
        if not dependent_id:
            errors.append("Missing dependent_id")
        elif dependent_id in existing_dependent_ids or dependent_id in seen_ids:
            errors.append("Duplicate dependent_id")
        for field in ("dependent_name", "gender", "date_of_birth", "relationship", "record_status"):
            if not record.get(field):
                errors.append(f"Missing required field: {field}")

        email = record.get("email")
        if email:
            record["email"] = str(email).lower()
            if not EMAIL_PATTERN.fullmatch(str(record["email"])):
                errors.append("Invalid email format")
        original_dob = record.get("date_of_birth")
        record["date_of_birth"] = _standardize_date(original_dob)
        if original_dob and record["date_of_birth"] is None:
            errors.append("Invalid date_of_birth")
        if record.get("sum_insured"):
            coverage = _coverage_amount(record["sum_insured"])
            if coverage is None:
                errors.append("Invalid sum_insured; expected a positive whole number")
            record["sum_insured"] = coverage
        if record.get("mobile_number"):
            record["mobile_number"] = standardize_phone(record["mobile_number"])
        if record.get("gender"):
            record["gender"] = str(record["gender"]).title()
        for field in ("created_at", "updated_at"):
            if record.get(field):
                standardized = _standardize_timestamp(record[field])
                if standardized is None:
                    errors.append(f"Invalid {field}")
                record[field] = standardized
        if str(record.get("record_status", "")).lower() != "active":
            errors.append("Dependent record_status must be active")
        else:
            record["record_status"] = "active"

        if errors:
            rejected.append(_rejection(source_record, errors))
            continue
        seen_ids.add(str(dependent_id))
        accepted.append(record)

    return ValidationResult(accepted=accepted, rejected=rejected)


def _rejection(source_record: dict[str, Any], errors: list[str]) -> dict[str, str]:
    return {
        "original_record": json.dumps(source_record, default=str, ensure_ascii=True),
        "error_reason": "; ".join(dict.fromkeys(errors)),
    }