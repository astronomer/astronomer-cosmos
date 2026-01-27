{{
    config(
        materialized='view'
    )
}}

-- Report: Executive revenue summary
-- Aggregates from finance-owned fact tables
with revenue as (
    select * from {{ ref('fct_revenue') }}
),

customer_revenue as (
    select * from {{ ref('fct_customer_revenue') }}
)

select
    -- Overall metrics
    (select sum(gross_revenue) from revenue) as total_revenue,
    (select sum(gross_profit) from revenue) as total_profit,
    (select sum(units_sold) from revenue) as total_units_sold,
    (select count(distinct order_date) from revenue) as days_with_orders,

    -- Customer metrics
    (select count(*) from customer_revenue) as total_customers,
    (select count(*) from customer_revenue where total_orders > 0) as active_customers,
    (select avg(lifetime_value) from customer_revenue where lifetime_value > 0) as avg_customer_ltv,

    -- Tier breakdown
    (select count(*) from customer_revenue where customer_tier = 'Gold') as gold_customers,
    (select count(*) from customer_revenue where customer_tier = 'Silver') as silver_customers,
    (select count(*) from customer_revenue where customer_tier = 'Bronze') as bronze_customers
