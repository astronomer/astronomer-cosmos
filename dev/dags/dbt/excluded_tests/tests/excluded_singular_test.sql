{{ config(tags=['excluded']) }}

select id from {{ ref('model_a') }} where id < 0
