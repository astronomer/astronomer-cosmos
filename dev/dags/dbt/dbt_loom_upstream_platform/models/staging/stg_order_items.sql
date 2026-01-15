{{
    config(
        materialized='view',
        access='public'
    )
}}

-- Staging model for order items - exposed publicly for cross-project reference
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as line_total
from {{ ref('raw_order_items') }}
