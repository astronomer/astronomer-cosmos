with base as (
    select
        1 as id,
        'original_row' as description

)

select * from base
union all
select
    2 as id,
    'added_row' as description
