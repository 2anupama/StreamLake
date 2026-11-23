"""Opt-in authenticated GCS and BigQuery smoke test; never runs by default."""

import os
import uuid

import pytest


@pytest.mark.skipif(os.getenv("RUN_GCP_INTEGRATION") != "1", reason="set RUN_GCP_INTEGRATION=1 to use authenticated GCP")
def test_authenticated_gcs_and_bigquery_smoke(tmp_path):
    from google.cloud import bigquery

    from streamlake.common.settings import Settings
    from streamlake.storage import GCSStorageAdapter
    from streamlake.warehouse import BigQueryWarehouse

    settings = Settings()
    if not settings.gcp_project_id or not settings.gcs_bucket:
        pytest.fail("GCP_PROJECT_ID and GCS_BUCKET must be set for the opt-in integration test")
    client = __import__("google.cloud.storage", fromlist=["Client"]).Client(project=settings.gcp_project_id)
    adapter = GCSStorageAdapter(settings.gcs_bucket, prefix="streamlake-integration-test", client=client)
    object_key = f"smoke-{uuid.uuid4().hex}.txt"
    source = tmp_path / "smoke.txt"
    source.write_text("streamlake gcp smoke test")
    try:
        uri = adapter.upload_file(source, object_key)
        assert adapter.get_metadata(object_key).size == source.stat().st_size
        destination = adapter.download_file(object_key, tmp_path / "downloaded.txt")
        assert destination.read_text() == source.read_text()
        assert uri in [f"gs://{settings.gcs_bucket}/{key}" for key in adapter.list_files("streamlake-integration-test")]
        warehouse = BigQueryWarehouse(settings.gcp_project_id, settings.gcp_region)
        rows = warehouse.query("SELECT @value AS value", [bigquery.ScalarQueryParameter("value", "STRING", "ok")])
        assert rows[0]["value"] == "ok"
    finally:
        client.bucket(settings.gcs_bucket).blob(f"streamlake-integration-test/{object_key}").delete()
