
    {{ config(materialized='table') }}

    select * from {{ ref('model_3375') }}
    