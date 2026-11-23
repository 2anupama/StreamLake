select employee_bk
from {{ ref('dim_employee') }}
group by employee_bk
having count(*) filter (where is_current) <> 1
