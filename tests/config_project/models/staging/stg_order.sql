{{
    config(
        materialized='incremental',
        tags='finance'
    )
}}
SELECT *
FROM {{ source('schema', 'order') }}
