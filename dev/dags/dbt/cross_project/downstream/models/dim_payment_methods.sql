{{
    config(
        materialized='table'
    )
}}

-- Dimension table: Payment method analysis
-- Uses cross-project refs to upstream via dbt-loom
with orders as (
    select * from {{ ref('upstream', 'int_orders_enriched') }}
)

select
    payment_method,
    count(*) as total_transactions,
    count(case when status = 'completed' then 1 end) as completed_transactions,
    count(case when status = 'cancelled' then 1 end) as cancelled_transactions,
    count(case when status = 'pending' then 1 end) as pending_transactions,
    sum(case when status = 'completed' then order_total else 0 end) as total_revenue,
    avg(case when status = 'completed' then order_total end) as avg_order_value
from orders
group by payment_method
