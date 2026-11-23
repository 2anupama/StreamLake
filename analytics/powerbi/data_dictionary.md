# Gold data dictionary and star schema

All counts below describe table grains, not necessarily unique employees.
Business keys are source identifiers. Current employee counts must filter
`dim_employee.is_current`; active employee counts must also filter
`record_status = 'active'`.

| Table | Grain | Primary/business key | Purpose |
|---|---|---|---|
| `dim_employee` | One row per employee SCD Type 2 version | `employee_sk` (surrogate), `employee_bk` (business) | Employee attributes, effective interval, current/deleted status |
| `dim_company` | One row per company | `company_sk`, `company_bk` | Company attributes and current aggregate measures |
| `dim_policy` | One row per policy/company | `policy_sk`, `policy_bk` | Policy attributes and enrollment totals |
| `dim_date` | One row per calendar date | `date_sk`, `date_day` | Calendar slicing |
| `dim_department` | One row per department | `department_sk`, `department_name` | Department reporting |
| `fact_employee_enrollment` | One row per employee enrollment business key/date | `enrollment_sk`; `employee_bk` is business key | Enrollment date and insured amount |
| `fact_employee_changes` | One row per source change event | `employee_change_sk`, `change_bk` | Insert/update/delete history |
| `fact_policy_utilization` | One row per policy/company | `policy_utilization_sk` | Policy enrollment and insured totals |
| `employee_enrollment_summary` | One row per company/policy | `(company_id, policy_id)` | Enrollment count, coverage, first date |
| `company_employee_count` | One row per company | `company_id` | Active employee count |
| `dependent_distribution` | One row per company/policy/relationship | `(company_id, policy_id, relationship)` | Dependent count aggregate |
| `sum_insured_distribution` | One row per coverage band | `sum_insured_band` | Enrollment and coverage totals by band |
| `monthly_employee_additions` | One row per month | `month` | Monthly insert count |
| `monthly_employee_deletions` | One row per month | `month` | Monthly delete count |
| `policy_wise_enrollment` | One row per policy/company | `(policy_id, company_id)` | Enrollment and coverage totals |
| `pipeline_metrics` | One row per observed metric | `metric_id` | Phase 6 actual pipeline metric history; operational database |

## Relationships

Recommended conceptual star: `dim_date` filters enrollment facts through
`enrollment_date`; employee attributes use the employee business key and
effective date to select the appropriate SCD2 version; company and policy
dimensions filter enrollment facts through `company_id` and `policy_id`.
Employee change facts join to employee history by `employee_bk` and to date by
the date of `event_timestamp`. The current dbt `fact_employee_enrollment` and
`fact_employee_changes` expose business keys rather than surrogate foreign keys,
and the change fact does not currently contain company or policy keys. Do not
create misleading physical FK relationships in Power BI; use the documented
business-key joins or extend the dbt facts first. `dependent_distribution` is
already aggregated and has no employee-level FK. Operational metrics are a
separate fact-like table and should not be joined to employee dimensions.

There is no dependent-level Gold fact in the current project. The dependent
dashboard is therefore limited to company/policy/relationship aggregates.
