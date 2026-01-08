-- This model uses cross-project references to platform_project models
select
    c.customer_id,
    c.customer_name,
    sum(o.amount) as total_revenue
from {{ ref('platform_project', 'stg_customers') }} c
left join {{ ref('platform_project', 'stg_orders') }} o
    on c.customer_id = o.customer_id
group by 1, 2
