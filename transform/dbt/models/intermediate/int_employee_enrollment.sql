select employee_id, company_id, policy_id, department, joining_date, sum_insured, record_status,
       source_batch_id, source_file, updated_at
from {{ ref('stg_employees') }}
