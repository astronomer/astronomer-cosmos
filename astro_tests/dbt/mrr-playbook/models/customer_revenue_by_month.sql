with subscription_periods as (

    select * from {{ ref('subscription_periods') }}

),

months as (

    select * from {{ ref('util_months') }}

),

-- determine when a given account had its first and last (or most recent) month
customers as (

    {% if var('conn_type') != 'bigquery' %}
      select
          customer_id,
          date_trunc('month', min(start_date)) as date_month_start,
          date_trunc('month', max(end_date)) as date_month_end
      from subscription_periods
      group by 1
    {% else %}
      select
          customer_id,
          date_trunc(min(start_date), MONTH) as date_month_start,
          date_trunc(max(end_date), MONTH) as date_month_end
      from subscription_periods
      group by 1
    {% endif %}
),

-- create one record per month between a customer's first and last month
-- (example of a date spine)
customer_months as (

    select
        customers.customer_id,
        months.date_month

    from customers

    inner join months
        -- all months after start date
        on  months.date_month >= customers.date_month_start
        -- and before end date
        and months.date_month < customers.date_month_end

),

-- join the account-month spine to MRR base model, pulling through most recent dates
-- and plan info for month rows that have no invoices (i.e. churns)
joined as (

    select
        customer_months.date_month,
        customer_months.customer_id,
        coalesce(subscription_periods.monthly_amount, 0) as mrr

    from customer_months

    left join subscription_periods
        on customer_months.customer_id = subscription_periods.customer_id
        -- month is after a subscription start date
        and customer_months.date_month >= subscription_periods.start_date
        -- month is before a subscription end date (and handle null case)
        and (customer_months.date_month < subscription_periods.end_date
                or subscription_periods.end_date is null)

),

final as (

    select
        date_month,
        customer_id,
        mrr,

        mrr > 0 as is_active,

        -- calculate first and last months
        min(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) as first_active_month,

        max(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) as last_active_month,

        -- calculate if this record is the first or last month
        case
          when min(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) = date_month then true
          else false end as is_first_month,
        case
          when max(case when mrr > 0 then date_month end) over (
            partition by customer_id
        ) = date_month then true
          else false end as is_last_month

    from joined

)

select * from final
