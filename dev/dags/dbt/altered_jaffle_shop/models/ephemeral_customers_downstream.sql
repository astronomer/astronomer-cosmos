-- Downstream of the ephemeral model. dbt inlines ephemeral_customers as a CTE here, so the
-- customers -> ephemeral_customers -> ephemeral_customers_downstream lineage flows through the
-- ephemeral node even though it is never materialized in the warehouse.
select * from {{ ref('ephemeral_customers') }}
