"""Local and GCS storage adapters with a common file and Parquet interface."""

from __future__ import annotations

import os
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd


@dataclass(frozen=True)
class ObjectMetadata:
    name: str
    size: int | None
    updated: str | None
    content_type: str | None
    generation: str | None
    uri: str


class StorageAdapter(Protocol):
    """Operations shared by local filesystem and GCS storage."""

    def upload_file(self, source: Path, relative_path: str | Path) -> str: ...
    def download_file(self, relative_path: str | Path, destination: Path) -> Path: ...
    def list_files(self, prefix: str = "") -> list[str]: ...
    def get_metadata(self, relative_path: str | Path) -> ObjectMetadata: ...
    def write_parquet(self, frame: pd.DataFrame, relative_path: Path) -> str: ...


class LocalParquetStorage:
    """Write and inspect files beneath a local filesystem root."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, relative_path: str | Path) -> Path:
        relative = Path(relative_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Storage paths must stay beneath the configured root")
        return self.root / relative

    def upload_file(self, source: Path, relative_path: str | Path) -> str:
        import shutil

        target = self._path(relative_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        return str(target)

    def download_file(self, relative_path: str | Path, destination: Path) -> Path:
        import shutil

        source = self._path(relative_path)
        if not source.is_file():
            raise FileNotFoundError(source)
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        return destination

    def list_files(self, prefix: str = "") -> list[str]:
        root = self._path(prefix) if prefix else self.root
        if not root.exists():
            return []
        return sorted(str(path.relative_to(self.root)) for path in root.rglob("*") if path.is_file())

    def get_metadata(self, relative_path: str | Path) -> ObjectMetadata:
        path = self._path(relative_path)
        stat = path.stat()
        return ObjectMetadata(
            name=str(path.relative_to(self.root)), size=stat.st_size,
            updated=pd.Timestamp(stat.st_mtime, unit="s", tz="UTC").isoformat(),
            content_type=None, generation=None, uri=str(path),
        )

    def write_parquet(self, frame: pd.DataFrame, relative_path: Path) -> str:
        destination = self._path(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(destination, index=False, compression="snappy")
        return str(destination)

    def write_partitioned_frame(
        self, frame: pd.DataFrame, layer: str, ingestion_date: str,
        source_type: str, batch_id: str, part_number: int,
    ) -> Path:
        path = (Path(layer) / f"ingestion_date={ingestion_date}" / f"source_type={source_type}"
                / f"batch_id={batch_id}" / f"part-{part_number:05d}.parquet")
        return Path(self.write_parquet(frame, path))

    def write_query_parquet(self, connection, query: str, relative_path: Path) -> Path:
        destination = self._path(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.stem}-{uuid.uuid4().hex}.tmp.parquet")
        escaped_path = str(temporary).replace("'", "''")
        try:
            connection.execute(f"COPY ({query}) TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination


class GCSStorageAdapter:
    """GCS adapter using ADC, SDK retries, and bounded retries for transient errors."""

    def __init__(self, bucket_name: str, prefix: str = "", client=None, project_id: str | None = None,
                 credentials=None, max_attempts: int = 3) -> None:
        if not bucket_name:
            raise ValueError("GCS_BUCKET is required when STORAGE_BACKEND=gcp")
        if client is None:
            try:
                from google.cloud import storage
            except ImportError as error:
                raise RuntimeError("GCS support requires `pip install -e '.[gcp]'`") from error
            client = storage.Client(project=project_id, credentials=credentials)
        self.client = client
        self.bucket = self.client.bucket(bucket_name)
        self.prefix = prefix.strip("/")
        self.max_attempts = max(1, max_attempts)

    def _name(self, path: str | Path) -> str:
        relative = Path(path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Storage paths must stay beneath the configured bucket prefix")
        return "/".join(part for part in (self.prefix, self._lake_layout(relative).as_posix()) if part and part != ".")

    @staticmethod
    def _lake_layout(path: Path) -> Path:
        """Place batch entity data under the documented medallion bucket hierarchy."""
        parts = list(path.parts)
        if not parts:
            return path
        layer = parts[0]
        if layer in {"bronze", "silver"}:
            entity = "dependents" if "dependent" in "/".join(parts).lower() else "policies" if "polic" in "/".join(parts).lower() else "employees"
            suffix = parts[1:]
            if layer == "silver" and suffix and suffix[0] == "events":
                suffix = suffix[1:]
            return Path(layer, entity, *suffix)
        if layer == "gold":
            return Path("gold", "marts", *parts[1:])
        return path

    def _retry(self, operation):
        for attempt in range(self.max_attempts):
            try:
                return operation()
            except Exception:
                if attempt + 1 == self.max_attempts:
                    raise
                time.sleep(min(0.25 * (2**attempt), 2.0))

    def upload_file(self, source: Path, relative_path: str | Path) -> str:
        blob = self.bucket.blob(self._name(relative_path))
        self._retry(lambda: blob.upload_from_filename(str(source)))
        return f"gs://{self.bucket.name}/{blob.name}"

    def download_file(self, relative_path: str | Path, destination: Path) -> Path:
        blob = self.bucket.blob(self._name(relative_path))
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self._retry(lambda: blob.download_to_filename(str(destination)))
        return destination

    def list_files(self, prefix: str = "") -> list[str]:
        full_prefix = self._name(prefix) if prefix else self.prefix
        return sorted(blob.name for blob in self._retry(lambda: self.client.list_blobs(self.bucket, prefix=full_prefix)))

    def get_metadata(self, relative_path: str | Path) -> ObjectMetadata:
        blob = self.bucket.blob(self._name(relative_path))
        self._retry(blob.reload)
        return ObjectMetadata(
            name=blob.name, size=blob.size,
            updated=blob.updated.isoformat() if blob.updated else None,
            content_type=blob.content_type, generation=str(blob.generation) if blob.generation else None,
            uri=f"gs://{self.bucket.name}/{blob.name}",
        )

    def write_parquet(self, frame: pd.DataFrame, relative_path: Path) -> str:
        with tempfile.TemporaryDirectory() as temporary_dir:
            local_path = Path(temporary_dir) / "part.parquet"
            frame.to_parquet(local_path, index=False, compression="snappy")
            return self.upload_file(local_path, relative_path)

    def write_partitioned_frame(
        self, frame: pd.DataFrame, layer: str, ingestion_date: str,
        source_type: str, batch_id: str, part_number: int,
    ) -> str:
        path = (Path(layer) / f"ingestion_date={ingestion_date}" / f"source_type={source_type}"
                / f"batch_id={batch_id}" / f"part-{part_number:05d}.parquet")
        return self.write_parquet(frame, path)

    def write_query_parquet(self, connection, query: str, relative_path: Path) -> str:
        with tempfile.TemporaryDirectory() as temporary_dir:
            local_path = Path(temporary_dir) / "result.parquet"
            escaped = str(local_path).replace("'", "''")
            connection.execute(f"COPY ({query}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
            return self.upload_file(local_path, relative_path)


def create_storage_adapter(settings: Any) -> StorageAdapter:
    """Construct the configured storage adapter without cloud imports in local mode."""
    if settings.storage_backend == "local":
        return LocalParquetStorage(settings.data_root)
    if settings.storage_backend == "gcp":
        credentials = _credentials_from_path(settings.google_application_credentials)
        return GCSStorageAdapter(settings.gcs_bucket, project_id=settings.gcp_project_id, credentials=credentials)
    raise ValueError(f"Unsupported storage backend: {settings.storage_backend}")


def _credentials_from_path(path):
    if not path:
        return None
    try:
        from google.oauth2 import service_account
    except ImportError as error:
        raise RuntimeError("GCP credentials require `pip install -e '.[gcp]'`") from error
    return service_account.Credentials.from_service_account_file(str(path))
