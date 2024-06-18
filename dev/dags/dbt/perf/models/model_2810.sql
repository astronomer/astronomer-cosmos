
    {{ config(materialized='table') }}

    select * from {{ ref('model_2809') }}
    