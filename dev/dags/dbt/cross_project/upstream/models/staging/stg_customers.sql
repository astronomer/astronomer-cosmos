{{
    config(
        materialized='view',
        access='public'
    )
}}

-- Staging model for customers - exposed publicly for cross-project reference
select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    cast(created_at as date) as signup_date,
    country,
    segment
from {{ ref('raw_customers') }}
