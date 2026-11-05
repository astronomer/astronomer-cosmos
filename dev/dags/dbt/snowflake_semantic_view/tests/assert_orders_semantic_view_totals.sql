select *
from semantic_view(
    {{ ref('orders_semantic_view') }}
    metrics orders.total_revenue, orders.total_orders
)
where total_revenue <> 971.50 or total_orders <> 6
