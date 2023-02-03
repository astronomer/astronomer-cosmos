{{ dbt_utils.date_spine(
    datepart="month",
    start_date="'2018-01-01'::date",
    end_date="date_trunc('month', current_date)"
   )
}}
