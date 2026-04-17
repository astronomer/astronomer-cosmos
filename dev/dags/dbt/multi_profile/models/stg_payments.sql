-- This model uses the "secondary" ProfileConfig.
-- meta.cosmos.profile_config_key: "secondary" is declared in schema.yml.
with source as (
    select * from {{ source('jaffle_shop', 'raw_payments') }}
),

renamed as (
    select
        id             as payment_id,
        order_id,
        payment_method,
        amount
    from source
)

select * from renamed
