"""Schemas and source-type helpers for batch ingestion."""

import re
from enum import Enum
from pathlib import Path


class SourceType(str, Enum):
    ADDITIONS = "additions"
    UPDATES = "updates"
    DELETIONS = "deletions"
    DEPENDENTS = "dependents"


EMPLOYEE_COLUMNS = (
    "employee_id",
    "employee_name",
    "email",
    "mobile_number",
    "gender",
    "date_of_birth",
    "joining_date",
    "company_id",
    "company_name",
    "subsidiary",
    "department",
    "designation",
    "sum_insured",
    "policy_id",
    "relationship",
    "record_status",
    "created_at",
    "updated_at",
)

DEPENDENT_COLUMNS = (
    "employee_id",
    "dependent_id",
    "dependent_name",
    "email",
    "mobile_number",
    "gender",
    "date_of_birth",
    "company_id",
    "company_name",
    "policy_id",
    "sum_insured",
    "relationship",
    "record_status",
    "created_at",
    "updated_at",
)

REQUIRED_COLUMNS = {
    SourceType.ADDITIONS: EMPLOYEE_COLUMNS,
    SourceType.UPDATES: EMPLOYEE_COLUMNS,
    SourceType.DELETIONS: ("employee_id",),
    SourceType.DEPENDENTS: (
        "employee_id",
        "dependent_id",
        "dependent_name",
        "gender",
        "date_of_birth",
        "relationship",
        "record_status",
    ),
}


def normalize_column_name(value: object) -> str:
    """Normalize common spreadsheet header formatting to snake case."""
    return re.sub(r"\s+", "_", str(value).strip().lower())


def infer_source_type(path: Path, explicit: str | SourceType | None = None) -> SourceType:
    """Resolve a source type from an explicit value or a conventional filename."""
    if explicit is not None:
        try:
            return explicit if isinstance(explicit, SourceType) else SourceType(explicit)
        except ValueError as error:
            raise ValueError(f"Unsupported source type: {explicit}") from error

    stem = path.stem.lower()
    if "dependent" in stem:
        return SourceType.DEPENDENTS
    if re.search(r"delet|remov", stem):
        return SourceType.DELETIONS
    if re.search(r"update|correction|change", stem):
        return SourceType.UPDATES
    if re.search(r"addition|new|employee", stem):
        return SourceType.ADDITIONS
    raise ValueError(
        f"Cannot infer source type from {path.name!r}; pass --source-type explicitly"
    )