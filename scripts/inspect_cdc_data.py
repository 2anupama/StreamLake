"""Print CDC Bronze counts and representative current Silver rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def inspect(root: Path, limit: int) -> dict[str, object]:
    database_path = root / "streamlake_cdc_v2.duckdb"
    silver_path = root / "silver" / "cdc_employee_current_v2.parquet"
    bronze_glob = str(root / "bronze" / "cdc_v2" / "**" / "*.parquet")
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        result: dict[str, object] = {
            "database": str(database_path),
            "silver_file_exists": silver_path.exists(),
        }
        if connection.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'cdc_event_ledger'"
        ).fetchone()[0]:
            result["event_counts"] = connection.execute(
                "SELECT topic, operation, count(*) AS events FROM cdc_event_ledger "
                "GROUP BY topic, operation ORDER BY topic, operation"
            ).fetchdf().to_dict(orient="records")
            result["employee_state_counts"] = connection.execute(
                "SELECT record_status, count(*) AS employees FROM employee_current "
                "GROUP BY record_status ORDER BY record_status"
            ).fetchdf().to_dict(orient="records")
            result["silver_employee_sample"] = connection.execute(
                "SELECT employee_id, record_status, last_operation, source_lsn, data_json "
                "FROM employee_current ORDER BY updated_at DESC LIMIT ?",
                [limit],
            ).fetchdf().to_dict(orient="records")
        else:
            result["event_counts"] = []
            result["employee_state_counts"] = []
            result["silver_employee_sample"] = []

        bronze_files = list((root / "bronze" / "cdc_v2").glob("**/*.parquet"))
        result["bronze_file_count"] = len(bronze_files)
        if bronze_files:
            result["bronze_event_sample"] = connection.execute(
                'SELECT topic, "partition", "offset", operation, source_lsn, raw_payload '
                f"FROM read_parquet('{bronze_glob}') ORDER BY event_timestamp_ms DESC NULLS LAST LIMIT ?",
                [limit],
            ).fetchdf().to_dict(orient="records")
        else:
            result["bronze_event_sample"] = []
        return result
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    print(json.dumps(inspect(args.data_root, args.limit), indent=2, default=str))


if __name__ == "__main__":
    main()