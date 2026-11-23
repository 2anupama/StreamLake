from streamlake.orchestration.airflow_tasks import (
    failure_notification_callback,
    retry_notification_callback,
    filter_unprocessed_candidates,
)


def test_discovery_skips_completed_fingerprints_by_default():
    candidates = [
        {"source_file": "new.csv", "fingerprint": "new"},
        {"source_file": "done.csv", "fingerprint": "done"},
    ]

    assert filter_unprocessed_candidates(candidates, {"done"}) == [candidates[0]]


def test_explicit_reprocess_includes_completed_fingerprints():
    candidates = [{"source_file": "done.csv", "fingerprint": "done"}]

    assert filter_unprocessed_candidates(candidates, {"done"}, reprocess=True) == candidates


def test_empty_candidate_set_is_idempotent():
    assert filter_unprocessed_candidates([], {"done"}) == []


def test_failure_callback_logs_failure_without_external_configuration(caplog, monkeypatch):
    monkeypatch.delenv("STREAMLAKE_AIRFLOW_FAILURE_CALLBACK", raising=False)
    monkeypatch.setattr("streamlake.orchestration.airflow_tasks._record_airflow_event", lambda *args: None)
    monkeypatch.setattr("streamlake.orchestration.airflow_tasks.send_alert", lambda *args: False)

    failure_notification_callback({"exception": RuntimeError("expected test failure")})

    assert "Airflow task failed" in caplog.text
    assert "expected test failure" in caplog.text


def test_retry_callback_records_retry_without_email(monkeypatch):
    calls = []
    monkeypatch.setattr("streamlake.orchestration.airflow_tasks._record_airflow_event",
                        lambda context, metric, status: calls.append((metric, status)))
    retry_notification_callback({"task_instance": type("TI", (), {
        "dag": type("DAG", (), {"dag_id": "test-dag"})(), "task_id": "transform",
        "run_id": "run-1", "try_number": 2,
    })()})
    assert calls == [("airflow_task_retries", "retrying")]


def test_historical_airflow_ingestion_sets_explicit_reprocess(tmp_path, monkeypatch):
    import streamlake.orchestration.airflow_tasks as tasks

    calls = []

    class FakePipeline:
        settings = type("Settings", (), {"source_system": "local"})()

        def ingest_file(self, *args, **kwargs):
            calls.append(kwargs)
            return type("Result", (), {"as_dict": lambda self: {"rows_loaded": 1, "rows_rejected": 0,
                                                                  "rows_processed": 1, "rows_ingested": 1,
                                                                  "skipped": False}})()

    source = tmp_path / "employees_additions.csv"
    source.write_text("data")
    monkeypatch.setattr(tasks, "_pipeline", FakePipeline)
    result = tasks.ingest_candidates(
        {"input_dir": str(tmp_path), "backfill": True,
         "candidates": [{"relative_path": source.name, "source_type": "additions", "source_file": source.name}]},
        {"batch_id": "backfill-1", "started_at": "2026-01-01T00:00:00+00:00"},
    )
    assert calls[0]["reprocess"] is True
    assert result["rows_loaded"] == 1
