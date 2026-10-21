{{
    config(
        materialized='table'
    )
}}

-- Report: customers enriched with their order activity.
-- Uses a cross-project ref into project_b via dbt-loom.
select
    c.customer_id,
    c.name,
    c.country,
    count(o.order_id) as num_orders,
    sum(o.amount) as total_spent
from {{ ref('stg_a_customers') }} c
left join {{ ref('project_b', 'stg_b_orders') }} o on o.customer_id = c.customer_id
group by 1, 2, 3
