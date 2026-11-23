select employee_bk from {{ ref('dim_employee') }}
where (effective_end_date is not null and effective_end_date < effective_start_date)
   or (effective_end_timestamp is not null and effective_end_timestamp < effective_start_timestamp)
