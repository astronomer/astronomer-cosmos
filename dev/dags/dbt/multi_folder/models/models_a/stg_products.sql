-- Depends on seed: seeds_a/products.csv
select
    id as product_id,
    name as product_name
from {{ ref('products') }}
