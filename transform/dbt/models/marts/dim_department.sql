select md5(coalesce(department, 'unknown')) as department_sk,
       coalesce(department, 'unknown') as department_name,
       count(distinct employee_id) filter (where record_status <> 'deleted') as active_employee_count
from {{ ref('stg_employees') }} group by 1, 2
