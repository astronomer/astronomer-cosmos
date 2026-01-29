{{
    config(
        materialized='table'
    )
}}

-- Fact table: Daily revenue metrics
-- Uses cross-project refs to upstream via dbt-loom
with orders as (
    select * from {{ ref('upstream', 'int_orders_enriched') }}
    where status = 'completed'
),

order_items as (
    select * from {{ ref('upstream', 'stg_order_items') }}
),

products as (
    select * from {{ ref('upstream', 'stg_products') }}
)

select
    o.order_date,
    p.category as product_category,
    count(distinct o.order_id) as num_orders,
    sum(oi.quantity) as units_sold,
    sum(oi.line_total) as gross_revenue,
    sum(oi.quantity * p.cost_price) as cost_of_goods_sold,
    sum(oi.line_total) - sum(oi.quantity * p.cost_price) as gross_profit
from orders o
inner join order_items oi on o.order_id = oi.order_id
inner join products p on oi.product_id = p.product_id
group by 1, 2
