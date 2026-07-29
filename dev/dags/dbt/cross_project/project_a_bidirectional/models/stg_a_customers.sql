{{
    config(
        materialized='view',
        access='public'
    )
}}

-- Staging model for customers - exposed publicly for cross-project reference
select
    customer_id,
    name,
    country
from {{ ref('raw_a_customers') }}
