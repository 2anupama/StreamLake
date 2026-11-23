select md5(company_id) as company_sk, company_id as company_bk, company_name,
       active_employee_count, policy_count, total_sum_insured
from {{ ref('stg_companies') }}
