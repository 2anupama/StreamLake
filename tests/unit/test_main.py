"""Tests for the initial application startup path."""

import logging

from streamlake.common.logging_config import configure_logging
from streamlake.common.settings import Settings
from streamlake.main import main


def test_main_starts_successfully(monkeypatch, tmp_path) -> None:
    """The application should start with local settings and a writable log."""
    monkeypatch.setenv("ENABLE_FILE_LOGGING", "true")
    monkeypatch.setenv("LOG_FILE", str(tmp_path / "streamlake.log"))

    assert main([]) == 0
    assert (tmp_path / "streamlake.log").exists()


def test_logging_falls_back_to_console_when_file_target_is_invalid(tmp_path, capsys) -> None:
    """An invalid file target should not prevent application logging."""
    configure_logging(Settings(enable_file_logging=True, log_file=tmp_path))

    logging.getLogger("streamlake.test").info("console fallback active")

    assert "console fallback active" in capsys.readouterr().out
    assert len(logging.getLogger().handlers) == 1


def test_logging_reconfiguration_does_not_duplicate_handlers(tmp_path) -> None:
    """Repeated setup should leave one console and one file handler."""
    settings = Settings(log_file=tmp_path / "streamlake.log")

    configure_logging(settings)
    configure_logging(settings)

    assert len(logging.getLogger().handlers) == 2