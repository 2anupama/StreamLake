from streamlake.orchestration.airflow_tasks import (
    failure_notification_callback,
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

    failure_notification_callback({"exception": RuntimeError("expected test failure")})

    assert "Airflow task failed" in caplog.text
    assert "expected test failure" in caplog.text
