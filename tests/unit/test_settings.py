"""Tests for application configuration defaults and environment loading."""

from streamlake.common.settings import Settings


def test_settings_have_local_defaults() -> None:
    """Settings should support local development without external services."""
    settings = Settings(_env_file=None)

    assert settings.app_name == "StreamLake"
    assert settings.environment == "local"
    assert settings.postgres_host == "localhost"
    assert settings.gcp_project_id is None


def test_settings_load_environment_variables(monkeypatch) -> None:
    """Environment variables should override the typed defaults."""
    monkeypatch.setenv("APP_NAME", "StreamLakeTest")
    monkeypatch.setenv("POSTGRES_PORT", "55432")

    settings = Settings(_env_file=None)

    assert settings.app_name == "StreamLakeTest"
    assert settings.postgres_port == 55432