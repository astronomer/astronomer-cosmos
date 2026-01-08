{{ config(
    materialized='view',
    access='public'
) }}

select
    1 as order_id,
    1 as customer_id,
    100.00 as amount,
    '2024-01-15' as order_date
union all
select
    2 as order_id,
    2 as customer_id,
    250.00 as amount,
    '2024-01-20' as order_date
