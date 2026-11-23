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


def test_cloud_backends_are_opt_in_and_configurable(monkeypatch) -> None:
    monkeypatch.setenv("STORAGE_BACKEND", "gcp")
    monkeypatch.setenv("WAREHOUSE_BACKEND", "bigquery")
    monkeypatch.setenv("GCP_PROJECT_ID", "unit-project")
    monkeypatch.setenv("GCP_REGION", "europe-west1")
    monkeypatch.setenv("GCS_BUCKET", "unit-bucket")
    monkeypatch.setenv("BIGQUERY_DATASET", "streamlake_gold")

    settings = Settings(_env_file=None)

    assert settings.storage_backend == "gcp"
    assert settings.warehouse_backend == "bigquery"
    assert settings.gcp_project_id == "unit-project"
    assert settings.gcp_region == "europe-west1"
    assert settings.gcs_bucket == "unit-bucket"
    assert settings.bigquery_dataset == "streamlake_gold"
