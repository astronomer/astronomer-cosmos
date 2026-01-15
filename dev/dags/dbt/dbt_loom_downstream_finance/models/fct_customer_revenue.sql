{{
    config(
        materialized='table'
    )
}}

-- Fact table: Customer revenue analysis
-- Uses cross-project refs to dbt_loom_upstream_platform via dbt-loom
with customer_orders as (
    select * from {{ ref('dbt_loom_upstream_platform', 'int_customer_orders') }}
),

customers as (
    select * from {{ ref('dbt_loom_upstream_platform', 'stg_customers') }}
)

select
    c.customer_id,
    c.full_name,
    c.email,
    c.segment,
    c.country,
    c.signup_date,
    coalesce(co.total_orders, 0) as total_orders,
    coalesce(co.lifetime_value, 0) as lifetime_value,
    co.first_order_date,
    co.last_order_date,
    -- Calculate days since last order
    case
        when co.last_order_date is not null
        then current_date - co.last_order_date
        else null
    end as days_since_last_order,
    -- Customer tier based on LTV
    case
        when coalesce(co.lifetime_value, 0) >= 500 then 'Gold'
        when coalesce(co.lifetime_value, 0) >= 200 then 'Silver'
        when coalesce(co.lifetime_value, 0) > 0 then 'Bronze'
        else 'Prospect'
    end as customer_tier
from customers c
left join customer_orders co on c.customer_id = co.customer_id
