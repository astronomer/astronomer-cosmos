select
    order_id,
    customer_id,
    order_date,
    status,
    revenue
from {{ ref('raw_orders') }}
