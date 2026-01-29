{{
    config(
        materialized='view',
        access='public'
    )
}}

-- Intermediate model: Customer order summary
-- Exposed publicly for cross-project reference
select
    c.customer_id,
    c.full_name,
    c.email,
    c.segment,
    c.country,
    count(distinct o.order_id) as total_orders,
    sum(o.order_total) as lifetime_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from {{ ref('stg_customers') }} c
left join {{ ref('int_orders_enriched') }} o on c.customer_id = o.customer_id
where o.status = 'completed'
group by 1, 2, 3, 4, 5
