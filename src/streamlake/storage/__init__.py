"""Local and cloud storage interfaces and adapters."""

from streamlake.storage.adapters import (
    GCSStorageAdapter,
    LocalParquetStorage,
    ObjectMetadata,
    StorageAdapter,
    create_storage_adapter,
)

__all__ = [
    "GCSStorageAdapter",
    "LocalParquetStorage",
    "ObjectMetadata",
    "StorageAdapter",
    "create_storage_adapter",
]
