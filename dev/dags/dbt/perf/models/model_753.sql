
    {{ config(materialized='table') }}

    select * from {{ ref('model_752') }}
    