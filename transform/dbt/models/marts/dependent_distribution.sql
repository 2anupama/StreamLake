select company_id, policy_id, relationship, count(*) as dependent_count
from {{ ref('stg_dependents') }} group by company_id, policy_id, relationship
