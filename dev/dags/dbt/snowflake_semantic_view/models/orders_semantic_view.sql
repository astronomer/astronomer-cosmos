{{ config(materialized='semantic_view') }}
TABLES (
    orders AS {{ ref('orders') }}
        PRIMARY KEY (order_id)
)
FACTS (
    orders.order_key AS order_id,
    orders.revenue_amount AS revenue
)
DIMENSIONS (
    orders.ordered_on AS order_date,
    orders.order_status AS status
)
METRICS (
    orders.total_orders AS COUNT(orders.order_key),
    orders.total_revenue AS SUM(orders.revenue_amount)
)
