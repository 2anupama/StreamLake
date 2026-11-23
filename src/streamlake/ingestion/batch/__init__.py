"""Batch ingestion, validation, and local pipeline APIs."""

from streamlake.ingestion.batch.generator import generate_synthetic_data
from streamlake.ingestion.batch.pipeline import BatchPipeline, IngestionResult
from streamlake.ingestion.batch.schemas import SourceType

__all__ = ["BatchPipeline", "IngestionResult", "SourceType", "generate_synthetic_data"]