select e.employee_bk from {{ ref('dim_employee') }} e
left join {{ ref('dim_company') }} c on e.company_id = c.company_bk
where c.company_bk is null
