with source as (

    select * from {{ source('postgres_db', 'raw_payments') }}

),

force_seed_dep as (
    {#-
    This CTE is used to ensure tests wait for seeds to run if source_node_rendering = none
    #}
    select * from {{ ref('raw_customers') }}
),

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from source

)

select * from renamed
