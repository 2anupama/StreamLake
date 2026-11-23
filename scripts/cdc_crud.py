"""Issue a single employee INSERT, UPDATE, or DELETE for CDC demonstrations."""

from __future__ import annotations

import argparse
import uuid

from seed_postgres import connect


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    insert = commands.add_parser("insert")
    insert.add_argument("--employee-id", default=f"EMP-CDC-{uuid.uuid4().hex[:8].upper()}")
    insert.add_argument("--name", default="CDC Example Employee")
    insert.add_argument("--email", default="cdc.example@example.test")
    update = commands.add_parser("update")
    update.add_argument("--employee-id", required=True)
    update.add_argument("--email", default="cdc.updated@example.test")
    update.add_argument("--sum-insured", type=int, default=750_000)
    delete = commands.add_parser("delete")
    delete.add_argument("--employee-id", required=True)
    args = parser.parse_args()

    with connect() as connection, connection.cursor() as cursor:
        if args.operation == "insert":
            cursor.execute(
                """INSERT INTO employees (
                       employee_id, employee_name, email, mobile_number, gender,
                       date_of_birth, joining_date, company_id, company_name,
                       subsidiary, department, designation, sum_insured, policy_id
                   ) VALUES (%s, %s, %s, %s, %s, DATE '1990-01-01', CURRENT_DATE,
                             'C001', 'Northstar Health', 'Northstar West',
                             'Engineering', 'Analyst', 500000, 'POL-C001-01')
                   RETURNING employee_id, email""",
                (args.employee_id, args.name, args.email, "+14155550100", "Non-binary"),
            )
        elif args.operation == "update":
            cursor.execute(
                """UPDATE employees SET email = %s, sum_insured = %s,
                          updated_at = clock_timestamp()
                   WHERE employee_id = %s RETURNING employee_id, email, sum_insured""",
                (args.email, args.sum_insured, args.employee_id),
            )
        else:
            cursor.execute(
                "DELETE FROM employees WHERE employee_id = %s RETURNING employee_id",
                (args.employee_id,),
            )
        result = cursor.fetchone()
        if result is None:
            raise SystemExit(f"Employee not found: {getattr(args, 'employee_id', '')}")
    print({"operation": args.operation, "result": result})


if __name__ == "__main__":
    main()