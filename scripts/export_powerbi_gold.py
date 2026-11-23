"""Export the current DuckDB Gold marts as portable Power BI CSV snapshots."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "streamlake_phase3.duckdb"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "powerbi"
GOLD_TABLES = (
    "dim_employee", "dim_company", "dim_policy", "dim_date", "dim_department",
    "fact_employee_enrollment", "fact_employee_changes", "fact_policy_utilization",
    "employee_enrollment_summary", "company_employee_count", "dependent_distribution",
    "sum_insured_distribution", "monthly_employee_additions", "monthly_employee_deletions",
    "employee_changes_history", "policy_wise_enrollment",
)


def export(database: Path = DEFAULT_DATABASE, output: Path = DEFAULT_OUTPUT) -> list[Path]:
    if not database.is_file():
        raise FileNotFoundError(f"Gold DuckDB database not found: {database}")
    output.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database), read_only=True)
    try:
        available = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        missing = [table for table in GOLD_TABLES if table not in available]
        if missing:
            raise RuntimeError(f"Gold marts are missing; run dbt build first: {missing}")
        exported: list[Path] = []
        for table in GOLD_TABLES:
            target = output / f"{table}.csv"
            connection.execute(
                "COPY (SELECT * FROM \"" + table + "\") TO ? (HEADER, DELIMITER ',')",
                [str(target)],
            )
            exported.append(target)
        # Metrics live in the Phase 6 operational DuckDB. Export only observed data.
        ops_path = PROJECT_ROOT / "data" / "streamlake.duckdb"
        if ops_path.is_file():
            ops = duckdb.connect(str(ops_path), read_only=True)
            try:
                tables = {row[0] for row in ops.execute("SHOW TABLES").fetchall()}
                if "pipeline_metrics" in tables:
                    target = output / "pipeline_metrics.csv"
                    ops.execute(
                        "COPY (SELECT * FROM pipeline_metrics) TO ? (HEADER, DELIMITER ',')",
                        [str(target)],
                    )
                    exported.append(target)
            finally:
                ops.close()
        return exported
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    for path in export(args.database, args.output):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
