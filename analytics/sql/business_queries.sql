-- StreamLake Phase 7 business queries (DuckDB SQL).
-- Run with: duckdb data/streamlake_phase3.duckdb < analytics/sql/business_queries.sql

-- 1. Total employees: one current dimension row per business employee.
SELECT count(DISTINCT employee_bk) AS total_employees
FROM dim_employee
WHERE is_current;

-- 2. Active employees. Deleted rows remain in the dimension as tombstones.
SELECT count(DISTINCT employee_bk) AS active_employees
FROM dim_employee
WHERE is_current AND lower(record_status) = 'active';

-- 3. Total dependents. Sum the Gold aggregate because the current mart has no
-- dependent-level fact table.
SELECT coalesce(sum(dependent_count), 0) AS total_dependents
FROM dependent_distribution;

-- 4. Company-wise active employee count.
SELECT company_id, company_name, employee_count
FROM company_employee_count
ORDER BY employee_count DESC, company_name;

-- 5. Monthly employee additions, based on preserved employee change events.
SELECT date_trunc('month', event_timestamp)::DATE AS month,
       count(DISTINCT employee_bk) AS employee_additions
FROM fact_employee_changes
WHERE lower(operation) IN ('c', 'insert')
GROUP BY 1 ORDER BY 1;

-- 6. Monthly employee deletions.
SELECT date_trunc('month', event_timestamp)::DATE AS month,
       count(DISTINCT employee_bk) AS employee_deletions
FROM fact_employee_changes
WHERE lower(operation) IN ('d', 'delete')
GROUP BY 1 ORDER BY 1;

-- 7. Enrollment trend by enrollment month. Deleted employees are excluded.
SELECT date_trunc('month', enrollment_date)::DATE AS month,
       count(DISTINCT employee_bk) AS enrolled_employees,
       sum(sum_insured) AS total_sum_insured
FROM fact_employee_enrollment
WHERE record_status IS NULL OR lower(record_status) <> 'deleted'
GROUP BY 1 ORDER BY 1;

-- 8. Sum-insured distribution.
SELECT sum_insured_band, enrollment_count, total_sum_insured
FROM sum_insured_distribution
ORDER BY CASE sum_insured_band
    WHEN 'under_250k' THEN 1 WHEN '250k_to_499k' THEN 2
    WHEN '500k_to_999k' THEN 3 ELSE 4 END;

-- 9. Policy-wise enrollment.
SELECT policy_id, company_id, employee_count, total_sum_insured
FROM policy_wise_enrollment
ORDER BY employee_count DESC, company_id, policy_id;

-- 10. Company-wise dependent distribution.
SELECT company_id, policy_id, relationship, dependent_count
FROM dependent_distribution
ORDER BY company_id, policy_id, relationship;

-- 11. Additions versus deletions on one calendar axis.
WITH changes AS (
    SELECT date_trunc('month', event_timestamp)::DATE AS month,
           count(DISTINCT employee_bk) FILTER (WHERE lower(operation) IN ('c', 'insert')) AS additions,
           count(DISTINCT employee_bk) FILTER (WHERE lower(operation) IN ('d', 'delete')) AS deletions
    FROM fact_employee_changes
    GROUP BY 1
)
SELECT month, coalesce(additions, 0) AS additions, coalesce(deletions, 0) AS deletions,
       coalesce(additions, 0) - coalesce(deletions, 0) AS net_change
FROM changes ORDER BY month;

-- 12. Data-quality rejection trend from actual Phase 6 metric observations.
-- This query intentionally returns no rows if no executions have recorded the
-- metric yet; it does not synthesize a historical series from today's report.
SELECT date_trunc('month', observed_at)::DATE AS month,
       pipeline_name,
       metric_name,
       sum(metric_value) AS rejected_records,
       count(*) AS execution_observations
FROM pipeline_metrics
WHERE metric_name IN ('records_rejected', 'data_quality_rejected_records')
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
