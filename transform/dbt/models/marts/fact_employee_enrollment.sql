select md5(employee_id || '|' || coalesce(cast(joining_date as varchar), 'unknown')) as enrollment_sk,
       employee_id as employee_bk, company_id, policy_id, department,
       cast(joining_date as date) as enrollment_date, cast(sum_insured as decimal(18,2)) as sum_insured,
       record_status, source_batch_id
from {{ ref('int_employee_enrollment') }}
