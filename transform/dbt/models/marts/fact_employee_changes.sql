select event_id as change_bk, md5(employee_id || '|' || event_id) as employee_change_sk,
       employee_id as employee_bk, operation, event_timestamp, source_file, source_batch_id
from {{ ref('stg_employee_changes') }}
