{{ config(materialized='incremental', unique_key='employee_id', incremental_strategy='delete+insert') }}
select *, lower(trim(email)) as normalized_email
from read_parquet('{{ var("phase3_data_root") }}/employee_current/**/*.parquet', union_by_name=true)
{% if is_incremental() %}
where coalesce(ingestion_timestamp, current_timestamp) > (select coalesce(max(ingestion_timestamp), timestamp '1900-01-01') from {{ this }})
{% endif %}
