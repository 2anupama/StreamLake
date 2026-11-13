"""Application entry point for the StreamLake platform."""

import logging

from streamlake.common.logging_config import configure_logging
from streamlake.common.settings import Settings

logger = logging.getLogger(__name__)


def main() -> int:
    """Initialize application configuration and report successful startup."""
    settings = Settings()
    configure_logging(settings)
    logger.info("Starting %s in %s environment", settings.app_name, settings.environment)
    logger.info("StreamLake foundation is ready")
    return 0