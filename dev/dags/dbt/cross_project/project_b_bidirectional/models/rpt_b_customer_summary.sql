{{
    config(
        materialized='table'
    )
}}

-- Report: order activity enriched with customer info.
-- Uses a cross-project ref into project_a via dbt-loom.
select
    o.order_id,
    o.order_date,
    o.amount,
    c.name as customer_name,
    c.country as customer_country
from {{ ref('stg_b_orders') }} o
left join {{ ref('project_a', 'stg_a_customers') }} c on c.customer_id = o.customer_id
