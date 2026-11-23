with bounds as (
  select min(joining_date)::date as min_date, max(coalesce(effective_end_date, current_date))::date as max_date
  from {{ ref('stg_employee_history') }}
), calendar as (
  select unnest(generate_series(min_date, max_date, interval 1 day))::date as date_day from bounds
)
select cast(strftime(date_day, '%Y%m%d') as integer) as date_sk, date_day,
       year(date_day) as year, quarter(date_day) as quarter,
       month(date_day) as month, monthname(date_day) as month_name,
       day(date_day) as day_of_month, dayofweek(date_day) as day_of_week
from calendar
