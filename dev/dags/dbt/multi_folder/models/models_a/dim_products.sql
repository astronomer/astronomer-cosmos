-- Depends on model: models_a/stg_products.sql
select
    product_id,
    product_name,
    upper(product_name) as product_name_upper
from {{ ref('stg_products') }}
