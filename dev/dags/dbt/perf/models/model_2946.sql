
    {{ config(materialized='table') }}

    select * from {{ ref('model_2945') }}
    