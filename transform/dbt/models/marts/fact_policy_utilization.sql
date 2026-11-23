select md5(policy_id || '|' || company_id) as policy_utilization_sk,
       policy_id, company_id, enrolled_employee_count, total_sum_insured,
       total_sum_insured / nullif(enrolled_employee_count, 0) as average_sum_insured
from {{ ref('stg_policies') }}
