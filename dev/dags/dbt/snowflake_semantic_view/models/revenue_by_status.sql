select
    order_status,
    total_revenue,
    total_orders
from semantic_view(
    {{ ref('orders_semantic_view') }}
    dimensions orders.order_status
    metrics orders.total_revenue, orders.total_orders
)
