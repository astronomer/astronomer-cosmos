{{ config(materialized='ephemeral') }}

-- An ephemeral model: inlined as a CTE into downstream models and never written to the warehouse.
select * from {{ ref('customers') }}
