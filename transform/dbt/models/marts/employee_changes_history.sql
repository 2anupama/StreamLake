select employee_bk, operation, event_timestamp, source_file, source_batch_id
from {{ ref('fact_employee_changes') }}
