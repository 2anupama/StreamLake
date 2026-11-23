select company_id, policy_id, count(distinct employee_bk) as employee_count,
       sum(sum_insured) as total_sum_insured, min(enrollment_date) as first_enrollment_date
from {{ ref('fact_employee_enrollment') }} where record_status <> 'deleted'
group by company_id, policy_id
