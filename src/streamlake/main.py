"""Application entry point for the StreamLake platform."""

import logging
import sys
from collections.abc import Sequence

from streamlake.common.logging_config import configure_logging
from streamlake.common.settings import Settings

logger = logging.getLogger(__name__)


def main(argv: Sequence[str] | None = None) -> int:
    """Initialize logging and dispatch a batch command or local startup."""
    settings = Settings()
    configure_logging(settings)
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments:
        from streamlake.ingestion.batch.cli import run_cli

        return run_cli(arguments, settings)
    logger.info("Starting %s in %s environment", settings.app_name, settings.environment)
    logger.info("StreamLake is ready; use a batch command to process data")
    return 0