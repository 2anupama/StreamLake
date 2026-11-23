select date_trunc('month', event_timestamp)::date as month, count(distinct employee_bk) as employee_additions
from {{ ref('fact_employee_changes') }} where operation in ('c', 'insert') group by 1
