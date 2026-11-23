{% snapshot snapshot_employee %}
{{ config(unique_key='employee_id', strategy='check', check_cols=['employee_name', 'email', 'company_id', 'department', 'designation', 'policy_id', 'sum_insured', 'record_status'], invalidate_hard_deletes=True) }}
select * from {{ ref('stg_employees') }}
{% endsnapshot %}
