{{
    config(
        materialized='incremental',
        tags='hourly'
    )
}}
SELECT *
FROM {{ ref('customer') }}
