from streamlake.quality.framework import evaluate_quality


def test_quality_framework_reconciles_counts_and_reports_failures():
    rows = [
        {"employee_id": "E1", "email": "ada@example.test", "date_of_birth": "1990-01-01", "company_id": "C1"},
        {"employee_id": "E2", "email": "bad", "date_of_birth": "2099-01-01", "company_id": None},
        {"employee_id": "E2", "email": "bad", "date_of_birth": "2099-01-01", "company_id": None},
    ]
    report, accepted, rejected = evaluate_quality(
        rows, unique_keys=("employee_id",), required=("employee_id", "company_id"),
        reference_keys={"company_id": {"C1"}}, as_of_date="2025-01-01",
    )
    assert report["input_rows"] == report["accepted_rows"] + report["rejected_rows"]
    assert len(accepted) == 1
    assert len(rejected) == 2
    assert "duplicate_employee_id" in rejected[0]["quality_errors"]
    assert "invalid_email" in rejected[0]["quality_errors"]
