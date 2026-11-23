"""Typed application settings loaded from environment variables or `.env`."""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration with safe local defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = "StreamLake"
    environment: Literal["local", "test", "staging", "production"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    log_file: Path = Path("logs/streamlake.log")
    enable_file_logging: bool = True

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "streamlake"
    postgres_user: str = "streamlake"
    postgres_password: SecretStr | None = None

    kafka_bootstrap_servers: str = "localhost:9092"
    data_root: Path = Path("data")
    duckdb_path: Path = Path("data/streamlake.duckdb")
    ingestion_audit_path: Path = Path("logs/ingestion_audit.jsonl")
    batch_chunk_size: int = Field(default=2_000, gt=0)
    source_system: str = "local"

    gcp_project_id: str | None = None
    gcs_bucket: str | None = None
    bigquery_dataset: str | None = None