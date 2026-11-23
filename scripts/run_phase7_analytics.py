"""Execute and validate the Phase 7 business SQL against local Gold data."""

from __future__ import annotations

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE = PROJECT_ROOT / "data" / "streamlake_phase3.duckdb"
SQL_FILE = PROJECT_ROOT / "analytics" / "sql" / "business_queries.sql"
OUTPUT = PROJECT_ROOT / "analytics" / "sample_outputs.md"


def run() -> dict[str, list[tuple]]:
    if not DATABASE.is_file():
        raise FileNotFoundError(f"Gold DuckDB database not found: {DATABASE}")
    connection = duckdb.connect(str(DATABASE), read_only=True)
    try:
        ops_path = PROJECT_ROOT / "data" / "streamlake.duckdb"
        has_metrics = False
        if ops_path.is_file():
            ops = duckdb.connect(str(ops_path), read_only=True)
            try:
                has_metrics = "pipeline_metrics" in {
                    row[0] for row in ops.execute("SHOW TABLES").fetchall()
                }
            finally:
                ops.close()
        if has_metrics:
            connection.execute(f"ATTACH '{ops_path.as_posix()}' AS operational (READ_ONLY)")
            connection.execute("CREATE TEMP VIEW pipeline_metrics AS SELECT * FROM operational.pipeline_metrics")
        else:
            connection.execute("""CREATE TEMP VIEW pipeline_metrics AS
                SELECT CAST(NULL AS TIMESTAMP) AS observed_at,
                       CAST(NULL AS VARCHAR) AS pipeline_name,
                       CAST(NULL AS VARCHAR) AS metric_name,
                       CAST(NULL AS DOUBLE) AS metric_value
                WHERE false""")

        sql = SQL_FILE.read_text(encoding="utf-8")
        statements = connection.extract_statements(sql)
        results: dict[str, list[tuple]] = {}
        for index, statement in enumerate(statements, start=1):
            relation = connection.execute(statement.query)
            results[f"query_{index}"] = relation.fetchall()
        if len(results) != 12:
            raise AssertionError(f"Expected 12 analytics queries, got {len(results)}")

        totals = results["query_1"][0][0]
        active = results["query_2"][0][0]
        dependents = results["query_3"][0][0]
        if totals < active or active < 0 or dependents < 0:
            raise AssertionError("Employee/dependent KPI reconciliation failed")
        if sum(row[2] for row in results["query_4"]) != active:
            raise AssertionError("Company employee counts do not reconcile to active employees")
        if sum(row[3] for row in results["query_10"]) != dependents:
            raise AssertionError("Company dependent aggregates do not reconcile to total dependents")
        unique_additions = connection.execute("""SELECT count(DISTINCT employee_bk)
            FROM fact_employee_changes WHERE lower(operation) IN ('c','insert')""").fetchone()[0]
        if sum(row[1] for row in results["query_5"]) != unique_additions:
            raise AssertionError("Monthly additions do not reconcile to distinct insert events")

        labels = {
            "query_1": "Total employees", "query_2": "Active employees",
            "query_3": "Total dependents", "query_4": "Company-wise active employee count",
            "query_5": "Monthly additions", "query_6": "Monthly deletions",
            "query_7": "Enrollment trend", "query_8": "Sum-insured distribution",
            "query_9": "Policy-wise enrollment", "query_10": "Company-wise dependent distribution",
            "query_11": "Additions versus deletions", "query_12": "Data-quality rejection trend",
        }
        lines = ["# StreamLake Phase 7 Sample Analytical Outputs", "",
                 "Generated from `data/streamlake_phase3.duckdb` by `scripts/run_phase7_analytics.py`.",
                 "These are measured local Gold values; empty metric output means no rejection telemetry was recorded.", ""]
        for key, label in labels.items():
            lines.extend((f"## {label}", "", "```text"))
            rows = results[key]
            lines.extend([repr(row) for row in rows[:20]] or ["(no rows)"])
            if len(rows) > 20:
                lines.append(f"... {len(rows) - 20} additional rows")
            lines.extend(("```", ""))
        OUTPUT.write_text("\n".join(lines), encoding="utf-8")
        return results
    finally:
        connection.close()


if __name__ == "__main__":
    outputs = run()
    print(f"Executed {len(outputs)} analytics queries against {DATABASE}")
    print(f"Validated active employees={outputs['query_2'][0][0]}, dependents={outputs['query_3'][0][0]}")
    print(f"Sample outputs: {OUTPUT}")
