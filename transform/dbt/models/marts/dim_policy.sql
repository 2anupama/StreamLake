select md5(policy_id || '|' || company_id) as policy_sk, policy_id as policy_bk,
       company_id, company_name, enrolled_employee_count, total_sum_insured
from {{ ref('stg_policies') }}
