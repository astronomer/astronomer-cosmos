with ad_spend as (

    select * from {{ ref('ad_spend') }}

),

attribution as (

    select * from {{ ref('attribution_touches') }}

),

-- aggregate first as this is easier to debug / often leads to fewer fanouts
ad_spend_aggregated as (

    select
        date_trunc('month', date_day) as date_month,
        utm_source,

        sum(spend) as total_spend

    from ad_spend

    group by 1, 2

),

attribution_aggregated as (

    select
        date_trunc('month', converted_at) as date_month,
        utm_source,

        sum(linear_points) as attribution_points,
        sum(linear_revenue) as attribution_revenue

    from attribution

    group by 1, 2

),

joined as (

    select
        *,
        1.0 * nullif(total_spend, 0) / attribution_points as cost_per_acquisition,
        1.0 * attribution_revenue / nullif(total_spend, 0) as return_on_advertising_spend

    from attribution_aggregated

    full outer join ad_spend_aggregated
    using (date_month, utm_source)

)

select * from joined
order by date_month, utm_source
