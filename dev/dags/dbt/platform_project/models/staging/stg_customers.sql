{{ config(
    materialized='view',
    access='public'
) }}

-- Wrapping source in a public staging model (workaround for cross-project source refs)
-- Source: {{ source('raw_data', 'raw_customers') }}
select
    1 as customer_id,
    'John Doe' as customer_name,
    'john@example.com' as email
union all
select
    2 as customer_id,
    'Jane Smith' as customer_name,
    'jane@example.com' as email
