{{ config(materialized='metric_view') }}

select * from {{ ref('customers') }}
