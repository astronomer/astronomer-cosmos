{% if var('conn_type') != 'bigquery' %}
  {{ dbt_utils.date_spine(
    datepart="month",
    start_date="'2018-01-01'::date",
    end_date="date_trunc('month', current_date)"
     )
  }}
{% else %}
  {{ dbt_utils.date_spine(
      datepart="month",
      start_date="cast('2018-01-01' AS DATE)",
      end_date="date_trunc(current_date, MONTH)"
     )
  }}
{% endif %}
