"""Local and optional cloud storage adapters for batch outputs."""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from typing import Protocol

import pandas as pd


class StorageAdapter(Protocol):
    """Minimal interface for writing Parquet objects to a storage backend."""

    def write_parquet(self, frame: pd.DataFrame, relative_path: Path) -> str:
        """Write a dataframe and return its backend-qualified location."""


class LocalParquetStorage:
    """Write Parquet datasets under a local filesystem root."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write_parquet(self, frame: pd.DataFrame, relative_path: Path) -> str:
        destination = self.root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(destination, index=False, compression="snappy")
        return str(destination)

    def write_partitioned_frame(
        self,
        frame: pd.DataFrame,
        layer: str,
        ingestion_date: str,
        source_type: str,
        batch_id: str,
        part_number: int,
    ) -> Path:
        """Write one bounded chunk into the lake's Hive-style partition layout."""
        relative_path = (
            Path(layer)
            / f"ingestion_date={ingestion_date}"
            / f"source_type={source_type}"
            / f"batch_id={batch_id}"
            / f"part-{part_number:05d}.parquet"
        )
        return Path(self.write_parquet(frame, relative_path))

    def write_query_parquet(self, connection, query: str, relative_path: Path) -> Path:
        """Materialize a DuckDB query atomically as a Parquet file."""
        destination = self.root / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.stem}-{uuid.uuid4().hex}.tmp.parquet")
        escaped_path = str(temporary).replace("'", "''")
        try:
            connection.execute(
                f"COPY ({query}) TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        return destination


class GCSStorageAdapter:
    """Optional GCS implementation; ADC is only used when this is instantiated."""

    def __init__(self, bucket_name: str, prefix: str = "", client=None) -> None:
        try:
            from google.cloud import storage
        except ImportError as error:
            raise RuntimeError(
                "GCS support requires the optional 'gcp' dependencies; install streamlake[gcp]"
            ) from error

        self.client = client or storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.prefix = prefix.strip("/")

    def write_parquet(self, frame: pd.DataFrame, relative_path: Path) -> str:
        object_name = "/".join(part for part in (self.prefix, relative_path.as_posix()) if part)
        with tempfile.TemporaryDirectory() as temporary_dir:
            local_path = Path(temporary_dir) / "part.parquet"
            frame.to_parquet(local_path, index=False, compression="snappy")
            self.bucket.blob(object_name).upload_from_filename(str(local_path))
        return f"gs://{self.bucket.name}/{object_name}"


class BigQueryLoader:
    """Optional Parquet-to-BigQuery loader; it does not initialize GCP locally."""

    def __init__(self, client=None) -> None:
        try:
            from google.cloud import bigquery
        except ImportError as error:
            raise RuntimeError(
                "BigQuery support requires the optional 'gcp' dependencies; install streamlake[gcp]"
            ) from error

        self.bigquery = bigquery
        self.client = client or bigquery.Client()

    def load_parquet(
        self,
        parquet_path: Path,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> int:
        """Load a local Parquet file and return the number of rows loaded."""
        destination = f"{self.client.project}.{dataset_id}.{table_id}"
        job_config = self.bigquery.LoadJobConfig(
            source_format=self.bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            autodetect=True,
        )
        with Path(parquet_path).open("rb") as source:
            job = self.client.load_table_from_file(
                source,
                destination,
                job_config=job_config,
                rewind=True,
            )
        result = job.result()
        return int(result.output_rows or 0)