select
    status,
    measure(total_revenue) as total_revenue,
    measure(total_orders) as total_orders
from {{ ref('order_metrics') }}
group by status
