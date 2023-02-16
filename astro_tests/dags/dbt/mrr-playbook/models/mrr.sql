with unioned as (

    select * from {{ ref('customer_revenue_by_month') }}

    union all

    select * from {{ ref('customer_churn_month') }}

),

-- get prior month MRR and calculate MRR change
mrr_with_changes as (

    select
        *,

        coalesce(
            lag(is_active) over (partition by customer_id order by date_month),
            false
        ) as previous_month_is_active,

        coalesce(
            lag(mrr) over (partition by customer_id order by date_month),
            0
        ) as previous_month_mrr,

        mrr - coalesce(
            lag(mrr) over (partition by customer_id order by date_month),
            0
        ) as mrr_change

    from unioned

),

-- classify months as new, churn, reactivation, upgrade, downgrade (or null)
-- also add an ID column
final as (

    select
        {{  dbt_utils.surrogate_key('date_month', 'customer_id') }} as id,

        *,

        case
            when is_first_month
                then 'new'
            when not(is_active) and previous_month_is_active
                then 'churn'
            when is_active and not(previous_month_is_active)
                then 'reactivation'
            when mrr_change > 0 then 'upgrade'
            when mrr_change < 0 then 'downgrade'
        end as change_category,

        least(mrr, previous_month_mrr) as renewal_amount

    from mrr_with_changes

)

select * from final
