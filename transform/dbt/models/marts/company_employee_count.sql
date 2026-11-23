select company_bk as company_id, company_name, active_employee_count as employee_count
from {{ ref('dim_company') }}
