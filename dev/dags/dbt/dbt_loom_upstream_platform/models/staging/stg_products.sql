{{
    config(
        materialized='view',
        access='public'
    )
}}

-- Staging model for products - exposed publicly for cross-project reference
select
    product_id,
    product_name,
    category,
    cost_price,
    list_price,
    list_price - cost_price as margin,
    is_active
from {{ ref('raw_products') }}
