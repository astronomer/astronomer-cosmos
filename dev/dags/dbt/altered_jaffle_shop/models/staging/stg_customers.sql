with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ source('postgres_db', 'raw_customers') }}

),

force_seed_dep as (
    {#-
    This CTE is used to ensure tests wait for seeds to run if source_node_rendering = none
    #}
    select * from {{ ref('raw_customers') }}
),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
