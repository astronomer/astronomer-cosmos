{{
    config(
        materialized='view',
        access='public'
    )
}}

-- Staging model for orders - exposed publicly for cross-project reference
select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    amount
from {{ ref('raw_b_orders') }}
