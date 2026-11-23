select employee_bk, sum_insured from {{ ref('fact_employee_enrollment') }}
where record_status <> 'deleted' and (sum_insured is null or sum_insured <= 0)
