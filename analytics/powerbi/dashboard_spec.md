# Power BI report specification

Build five report pages over the exported Gold CSV tables. Use single-direction
dimension-to-fact relationships. Add `dim_company` and `dim_policy` slicers to
pages where their keys are present; use a between/range date slicer on the date
and monthly trend pages. Hide surrogate/hash keys from report authors.

## Employee overview

- KPI cards: total employees, active employees, total dependents, total insured.
- Bar: active employees by company.
- Donut: active enrollment by sum-insured band.
- Slicers: company, policy, department.
- Total employees counts current employee business keys; active employees
  additionally requires `record_status = active`. Deleted tombstones remain
  visible to historical reporting.

## Company enrollment

- Ranked bar: active employee count by company.
- Matrix: company, policy, enrolled count, total sum insured.
- Dependent table or stacked bar: relationship by company from
  `dependent_distribution`.
- Slicers: company and policy.

## Policy distribution

- Donut: employee enrollment by policy.
- Bar: sum insured by policy/company.
- Cards: enrolled employees, total sum insured, average sum insured.
- Slicers: company and policy.

## Monthly additions and deletions

- Line chart with month on the X-axis and additions/deletions as separate
  measures; optional clustered columns for net change.
- Line chart for enrollment counts by enrollment month.
- Date slicer and company/policy filters. The current event fact has employee,
  operation, and timestamp, but no company key; company filtering applies only
  to enrollment-based visuals unless a future change fact adds the company key.

## Data quality

- KPI: rejected records for the selected period.
- Line chart: monthly rejection observations by pipeline and metric name. Keep
  the metric-name series separate; ingestion rejection and downstream quality
  task rejection may describe overlapping rows.
- Optional table: source/batch audit and rejection details, if those tables are
  exported and access is appropriate.
- The line uses `pipeline_metrics` execution observations, not a fabricated
  backfilled series. It will be empty until Phase 6 telemetry has been
  generated. For row-level rejection analysis, use Quarantine data with
  restricted access because it may contain personal information.

## Suggested DAX measures

```DAX
Total Employees = CALCULATE(DISTINCTCOUNT(dim_employee[employee_bk]), dim_employee[is_current] = TRUE())
Active Employees = CALCULATE([Total Employees], dim_employee[record_status] = "active")
Total Dependents = SUM(dependent_distribution[dependent_count])
Total Sum Insured = SUM(fact_employee_enrollment[sum_insured])
Employee Additions = SUM(monthly_employee_additions[employee_additions])
Employee Deletions = SUM(monthly_employee_deletions[employee_deletions])
Rejected Records = SUM(pipeline_metrics[metric_value])
```

For the rejection KPI, select one rejection metric name at a time. Prefer the
provided SQL monthly aggregation if the source is not imported with all metric
types.
