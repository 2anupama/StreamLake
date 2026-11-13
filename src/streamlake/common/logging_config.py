"""Centralized console and rotating-file logging setup."""

import logging
import logging.config
from logging.handlers import RotatingFileHandler
from pathlib import Path

from streamlake.common.settings import Settings


def configure_logging(settings: Settings) -> None:
    """Configure application logging, falling back to console on file errors."""
    log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    formatter = logging.Formatter(log_format)
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"standard": {"format": log_format}},
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": settings.log_level,
                    "formatter": "standard",
                    "stream": "ext://sys.stdout",
                }
            },
            "root": {"level": settings.log_level, "handlers": ["console"]},
        }
    )

    if not settings.enable_file_logging:
        return

    try:
        Path(settings.log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            settings.log_file,
            maxBytes=10_485_760,
            backupCount=5,
            encoding="utf-8",
        )
    except OSError:
        logging.getLogger(__name__).exception(
            "Unable to configure log file %s; continuing with console logging",
            settings.log_file,
        )
        return

    file_handler.setLevel(settings.log_level)
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)