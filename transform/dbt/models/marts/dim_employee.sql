{{ config(materialized='table') }}
select employee_sk, employee_id as employee_bk, employee_name, lower(trim(email)) as email,
       mobile_number, gender, date_of_birth, joining_date, company_id, department,
       designation, policy_id, sum_insured, record_status, effective_start_date,
       effective_end_date, effective_start_timestamp, effective_end_timestamp,
       is_current, source_batch_id, source_file
from {{ ref('stg_employee_history') }}
