"""Command-line interface for synthetic data generation and batch operations."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Sequence

from streamlake.common.settings import Settings
from streamlake.ingestion.batch.generator import generate_synthetic_data
from streamlake.ingestion.batch.pipeline import BatchPipeline, IngestionResult
from streamlake.ingestion.batch.schemas import SourceType

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="streamlake", description="StreamLake local batch pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate-data", help="Generate synthetic healthcare-benefits CSV inputs")
    generate.add_argument("--output-dir", type=Path, default=Path("data/incoming"))
    generate.add_argument("--employees", type=int, default=10_000)
    generate.add_argument("--seed", type=int, default=42)

    ingest = commands.add_parser("ingest", help="Ingest one CSV or Excel file")
    ingest.add_argument("--file", type=Path, required=True)
    ingest.add_argument("--source-type", choices=[source.value for source in SourceType])
    ingest.add_argument("--source-system", default=None)

    run = commands.add_parser("run-batch", help="Run recognized incoming files in dependency order")
    run.add_argument("--input-dir", type=Path, default=Path("data/incoming"))

    validate = commands.add_parser("validate", help="Validate input file extensions and schemas")
    validate.add_argument("--input-dir", type=Path, default=Path("data/incoming"))

    commands.add_parser("report", help="Report current Silver/Gold state and recent batch history")
    return parser


def run_cli(argv: Sequence[str], settings: Settings | None = None) -> int:
    args = build_parser().parse_args(list(argv))
    settings = settings or Settings()
    try:
        if args.command == "generate-data":
            counts = generate_synthetic_data(args.output_dir, args.employees, args.seed)
            print(json.dumps({"output_dir": str(args.output_dir), "files": counts}, indent=2))
            return 0

        pipeline = BatchPipeline(settings)
        if args.command == "ingest":
            result = pipeline.ingest_file(args.file, args.source_type, args.source_system)
            _print_results([result])
            return 0
        if args.command == "run-batch":
            results = pipeline.run_batch(args.input_dir)
            _print_results(results)
            return 0
        if args.command == "validate":
            outcomes = pipeline.validate_inputs(args.input_dir)
            print(json.dumps(outcomes, indent=2, default=str))
            return 1 if any(item.get("status") == "invalid" for item in outcomes) else 0
        if args.command == "report":
            print(json.dumps(pipeline.report(), indent=2, default=str))
            return 0
    except Exception as error:
        logger.error("Batch command failed: %s", error, exc_info=True)
        return 2
    return 2


def _print_results(results: list[IngestionResult]) -> None:
    totals = {
        "rows_ingested": sum(result.rows_ingested for result in results),
        "rows_processed": sum(result.rows_processed for result in results),
        "rows_rejected": sum(result.rows_rejected for result in results),
        "rows_loaded": sum(result.rows_loaded for result in results),
    }
    print(json.dumps({"batches": [result.as_dict() for result in results], "totals": totals}, indent=2))