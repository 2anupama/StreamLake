"""Reusable in-memory business quality checks for local loads and tests."""

from __future__ import annotations

import re
from collections import Counter
from datetime import date
from typing import Iterable, Mapping

_EMAIL = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def evaluate_quality(
    rows: Iterable[Mapping], *, unique_keys: tuple[str, ...], required: tuple[str, ...],
    reference_keys: Mapping[str, set] | None = None, as_of_date: str | None = None,
) -> tuple[dict, list[dict], list[dict]]:
    """Return a reconciliation report, accepted rows, and error-annotated quarantine rows."""
    materialized = [dict(row) for row in rows]
    counts = Counter(tuple(row.get(key) for key in unique_keys) for row in materialized)
    accepted: list[dict] = []
    rejected: list[dict] = []
    refs = reference_keys or {}
    for row in materialized:
        errors: list[str] = []
        for field in required:
            if row.get(field) is None or (isinstance(row.get(field), str) and not row[field].strip()):
                errors.append(f"missing_{field}")
        key = tuple(row.get(field) for field in unique_keys)
        if counts[key] > 1:
            errors.append(f"duplicate_{'_'.join(unique_keys)}")
        email = row.get("email")
        if email and not _EMAIL.fullmatch(str(email).strip().lower()):
            errors.append("invalid_email")
        dob = row.get("date_of_birth")
        if dob:
            try:
                parsed_dob = dob if isinstance(dob, date) else date.fromisoformat(str(dob)[:10])
                if as_of_date and parsed_dob > date.fromisoformat(as_of_date):
                    errors.append("future_date_of_birth")
            except ValueError:
                errors.append("invalid_date_of_birth")
        for field, allowed in refs.items():
            if row.get(field) is not None and row[field] not in allowed:
                errors.append(f"unknown_{field}")
        amount = row.get("sum_insured")
        if amount is not None:
            try:
                if float(amount) <= 0:
                    errors.append("non_positive_sum_insured")
            except (TypeError, ValueError):
                errors.append("invalid_sum_insured")
        if errors:
            rejected.append({**row, "quality_errors": sorted(set(errors))})
        else:
            accepted.append(row)
    report = {
        "input_rows": len(materialized), "accepted_rows": len(accepted),
        "rejected_rows": len(rejected), "unique_keys": list(unique_keys),
        "required_fields": list(required),
        "source_target_reconciled": len(materialized) == len(accepted) + len(rejected),
    }
    return report, accepted, rejected
