select date_trunc('month', event_timestamp)::date as month, count(distinct employee_bk) as employee_deletions
from {{ ref('fact_employee_changes') }} where operation in ('d', 'delete') group by 1
