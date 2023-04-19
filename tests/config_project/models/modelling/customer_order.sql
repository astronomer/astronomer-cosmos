{{
    config(
        materialized='view',
        tags='daily'
    )
}}
SELECT
     c.address
    ,o.item
    ,o.value
FROM {{ ref('order') }} AS o
JOIN {{ ref('customer') }} AS c
