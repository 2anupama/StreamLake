select case when sum_insured < 250000 then 'under_250k'
            when sum_insured < 500000 then '250k_to_499k'
            when sum_insured < 1000000 then '500k_to_999k' else '1m_plus' end as sum_insured_band,
       count(*) as enrollment_count, sum(sum_insured) as total_sum_insured
from {{ ref('fact_employee_enrollment') }} where record_status <> 'deleted'
group by 1
