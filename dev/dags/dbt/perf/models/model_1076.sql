
    {{ config(materialized='table') }}

    select * from {{ ref('model_1075') }}
    