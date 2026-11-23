"""Generate synthetic employee-benefits input files for local testing."""

from __future__ import annotations

import random
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from faker import Faker

COMPANIES = (
    ("C001", "Northstar Health", "Northstar West"),
    ("C002", "Evergreen Systems", "Evergreen Services"),
    ("C003", "Summit Manufacturing", "Summit Components"),
    ("C004", "Harbor Financial", "Harbor Operations"),
    ("C005", "Cedar Retail Group", "Cedar Digital"),
)
DEPARTMENTS = (
    "Engineering",
    "Human Resources",
    "Finance",
    "Operations",
    "Customer Success",
    "Sales",
)
DESIGNATIONS = (
    "Analyst",
    "Associate",
    "Manager",
    "Senior Engineer",
    "Director",
    "Benefits Coordinator",
)
POLICIES = ("POL-100", "POL-200", "POL-300", "POL-400")
SUM_INSURED_VALUES = (250_000, 500_000, 750_000, 1_000_000, 1_500_000)


def generate_synthetic_data(
    output_dir: Path,
    employee_count: int = 10_000,
    seed: int = 42,
) -> dict[str, int]:
    """Write additions, updates, deletions, and dependent CSV files.

    ``employee_count`` is the number of unique baseline employees. The additions
    file also contains deliberate duplicate and invalid rows for quality tests.
    """
    if employee_count < 1:
        raise ValueError("employee_count must be at least 1")

    output_dir.mkdir(parents=True, exist_ok=True)
    faker = Faker("en_US")
    faker.seed_instance(seed)
    randomizer = random.Random(seed)
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    employees: list[dict[str, object]] = []
    for index in range(employee_count):
        company_id, company_name, subsidiary = randomizer.choice(COMPANIES)
        employees.append(
            {
                "employee_id": f"EMP-{index + 1:07d}",
                "employee_name": faker.name(),
                "email": faker.email(),
                "mobile_number": f"+1{randomizer.randint(2_000_000_000, 9_999_999_999)}",
                "gender": randomizer.choice(("Female", "Male", "Non-binary")),
                "date_of_birth": faker.date_of_birth(minimum_age=21, maximum_age=65).isoformat(),
                "joining_date": faker.date_between(start_date="-20y", end_date="today").isoformat(),
                "company_id": company_id,
                "company_name": company_name,
                "subsidiary": subsidiary,
                "department": randomizer.choice(DEPARTMENTS),
                "designation": randomizer.choice(DESIGNATIONS),
                "sum_insured": randomizer.choice(SUM_INSURED_VALUES),
                "policy_id": randomizer.choice(POLICIES),
                "relationship": "Self",
                "record_status": "active",
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        )

    additions = [dict(employee) for employee in employees]
    duplicate = dict(employees[0])
    additions.append(duplicate)

    invalid_email = dict(employees[min(1, employee_count - 1)])
    invalid_email.update(employee_id="EMP-BAD-EMAIL", email=" ")
    additions.append(invalid_email)

    invalid_date = dict(employees[min(2, employee_count - 1)])
    invalid_date.update(employee_id="EMP-BAD-DATE", date_of_birth="2023-02-30")
    additions.append(invalid_date)

    missing_company = dict(employees[min(3, employee_count - 1)])
    missing_company.update(employee_id="EMP-MISSING-COMPANY", company_id="")
    additions.append(missing_company)

    invalid_sum_insured = dict(employees[min(4, employee_count - 1)])
    invalid_sum_insured.update(employee_id="EMP-BAD-COVERAGE", sum_insured="not-a-number")
    additions.append(invalid_sum_insured)

    update_count = min(max(employee_count // 20, 1), employee_count)
    update_employees = employees[:update_count]
    updates: list[dict[str, object]] = []
    for index, employee in enumerate(update_employees):
        updated = dict(employee)
        updated["email"] = f"corrected.{index + 1}@{updated['company_id'].lower()}.example"
        updated["sum_insured"] = SUM_INSURED_VALUES[(index + 1) % len(SUM_INSURED_VALUES)]
        updated["policy_id"] = POLICIES[(index + 1) % len(POLICIES)]
        updated["updated_at"] = timestamp
        updates.append(updated)

    deletion_count = max(employee_count // 100, 1)
    deletion_count = min(deletion_count, employee_count)
    deletions = [
        {"employee_id": employee["employee_id"], "record_status": "deleted", "updated_at": timestamp}
        for employee in employees[-deletion_count:]
    ]

    dependents: list[dict[str, object]] = []
    dependent_index = 0
    for employee_index, employee in enumerate(employees):
        if employee_index % 4:
            continue
        dependent_count = 2 if employee_index % 20 == 0 else 1
        for _ in range(dependent_count):
            dependent_index += 1
            company_id = str(employee["company_id"])
            dependents.append(
                {
                    "employee_id": employee["employee_id"],
                    "dependent_id": f"DEP-{dependent_index:08d}",
                    "dependent_name": faker.name(),
                    "email": faker.email() if dependent_index % 3 == 0 else "",
                    "mobile_number": f"+1{randomizer.randint(2_000_000_000, 9_999_999_999)}",
                    "gender": randomizer.choice(("Female", "Male", "Non-binary")),
                    "date_of_birth": faker.date_of_birth(minimum_age=1, maximum_age=70).isoformat(),
                    "company_id": company_id,
                    "company_name": employee["company_name"],
                    "policy_id": employee["policy_id"],
                    "sum_insured": employee["sum_insured"],
                    "relationship": randomizer.choice(("Spouse", "Child", "Parent")),
                    "record_status": "active",
                    "created_at": timestamp,
                    "updated_at": timestamp,
                }
            )

    if dependents:
        dependents.append(dict(dependents[0]))
    orphan = dict(dependents[0]) if dependents else {}
    orphan.update(
        employee_id="EMP-UNKNOWN",
        dependent_id="DEP-ORPHAN",
        dependent_name="Unlinked Dependent",
    )
    dependents.append(orphan)

    files = {
        "employees_additions.csv": additions,
        "employees_updates.csv": updates,
        "employees_deletions.csv": deletions,
        "employee_dependents.csv": dependents,
    }
    counts: dict[str, int] = {}
    for filename, records in files.items():
        path = output_dir / filename
        pd.DataFrame.from_records(records).to_csv(path, index=False)
        counts[filename] = len(records)
    return counts