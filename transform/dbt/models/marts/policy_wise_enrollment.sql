select policy_bk as policy_id, company_id, enrolled_employee_count as employee_count,
       total_sum_insured
from {{ ref('dim_policy') }}
