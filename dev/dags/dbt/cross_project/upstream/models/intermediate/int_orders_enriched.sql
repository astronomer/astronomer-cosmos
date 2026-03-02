{{
    config(
        materialized='view',
        access='public'
    )
}}

-- Intermediate model: Orders enriched with line items and totals
-- Exposed publicly for cross-project reference
with order_totals as (
    select
        order_id,
        sum(line_total) as order_total,
        sum(quantity) as total_items
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.payment_method,
    coalesce(totals.order_total, 0) as order_total,
    coalesce(totals.total_items, 0) as total_items
from {{ ref('stg_orders') }} o
left join order_totals totals on o.order_id = totals.order_id
