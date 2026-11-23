"""Seed the local CDC PostgreSQL database with synthetic benefits records."""

from __future__ import annotations

import argparse
import os
import random

import psycopg
from faker import Faker

COMPANIES = (
    ("C001", "Northstar Health", "Northstar West"),
    ("C002", "Evergreen Systems", "Evergreen Services"),
    ("C003", "Summit Manufacturing", "Summit Components"),
    ("C004", "Harbor Financial", "Harbor Operations"),
    ("C005", "Cedar Retail Group", "Cedar Digital"),
)
POLICY_VALUES = (250_000, 500_000, 750_000, 1_000_000)
DEPARTMENTS = ("Engineering", "Human Resources", "Finance", "Operations", "Sales")
DESIGNATIONS = ("Analyst", "Associate", "Manager", "Senior Engineer", "Director")


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "streamlake_source"),
        user=os.environ.get("POSTGRES_USER", "streamlake"),
        password=os.environ.get("POSTGRES_PASSWORD") or "streamlake_local_only",
        connect_timeout=10,
    )


def seed(employee_count: int, seed: int) -> dict[str, int]:
    if employee_count < 1:
        raise ValueError("employee_count must be greater than zero")
    faker = Faker("en_US")
    faker.seed_instance(seed)
    randomizer = random.Random(seed)

    companies = [
        {"company_id": company_id, "company_name": name, "subsidiary": subsidiary}
        for company_id, name, subsidiary in COMPANIES
    ]
    policies = [
        {
            "policy_id": f"POL-{company_id}-{policy_index:02d}",
            "company_id": company_id,
            "policy_name": f"{name} Benefits {policy_index}",
            "sum_insured": coverage,
        }
        for company_id, name, _ in COMPANIES
        for policy_index, coverage in enumerate(POLICY_VALUES, start=1)
    ]
    policies_by_company: dict[str, list[dict[str, object]]] = {}
    for policy in policies:
        policies_by_company.setdefault(str(policy["company_id"]), []).append(policy)

    employees: list[tuple[object, ...]] = []
    employee_dependents: list[tuple[object, ...]] = []
    for index in range(1, employee_count + 1):
        company_id, company_name, subsidiary = randomizer.choice(COMPANIES)
        policy = randomizer.choice(policies_by_company[company_id])
        employee_id = f"EMP-{index:08d}"
        employees.append(
            (
                employee_id,
                faker.name(),
                faker.safe_email(),
                f"+1{randomizer.randint(2_000_000_000, 9_999_999_999)}",
                randomizer.choice(("Female", "Male", "Non-binary")),
                faker.date_of_birth(minimum_age=21, maximum_age=65),
                faker.date_between(start_date="-20y", end_date="today"),
                company_id,
                company_name,
                subsidiary,
                randomizer.choice(DEPARTMENTS),
                randomizer.choice(DESIGNATIONS),
                policy["sum_insured"],
                policy["policy_id"],
            )
        )
        if index % 4 == 0:
            employee_dependents.append(
                (
                    f"DEP-{index:08d}-01",
                    employee_id,
                    faker.name(),
                    faker.safe_email(),
                    f"+1{randomizer.randint(2_000_000_000, 9_999_999_999)}",
                    randomizer.choice(("Female", "Male", "Non-binary")),
                    faker.date_of_birth(minimum_age=1, maximum_age=70),
                    randomizer.choice(("Spouse", "Child", "Parent")),
                )
            )

    with connect() as connection, connection.cursor() as cursor:
        cursor.executemany(
            """INSERT INTO companies (company_id, company_name, subsidiary)
               VALUES (%s, %s, %s) ON CONFLICT (company_id) DO NOTHING""",
            [(row["company_id"], row["company_name"], row["subsidiary"]) for row in companies],
        )
        cursor.executemany(
            """INSERT INTO policies (policy_id, company_id, policy_name, sum_insured)
               VALUES (%s, %s, %s, %s) ON CONFLICT (policy_id) DO NOTHING""",
            [
                (row["policy_id"], row["company_id"], row["policy_name"], row["sum_insured"])
                for row in policies
            ],
        )
        cursor.executemany(
            """INSERT INTO employees (
                   employee_id, employee_name, email, mobile_number, gender,
                   date_of_birth, joining_date, company_id, company_name, subsidiary,
                   department, designation, sum_insured, policy_id
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (employee_id) DO NOTHING""",
            employees,
        )
        cursor.executemany(
            """INSERT INTO dependents (
                   dependent_id, employee_id, dependent_name, email, mobile_number,
                   gender, date_of_birth, relationship
               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (dependent_id) DO NOTHING""",
            employee_dependents,
        )
    return {"companies": len(companies), "policies": len(policies), "employees": len(employees), "dependents": len(employee_dependents)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--employees", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    print(seed(args.employees, args.seed))


if __name__ == "__main__":
    main()