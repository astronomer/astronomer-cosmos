select *
from (
    select
        measure(total_revenue) as total_revenue,
        measure(total_orders) as total_orders
    from {{ ref('order_metrics') }}
)
where total_revenue <> 971.50 or total_orders <> 6
