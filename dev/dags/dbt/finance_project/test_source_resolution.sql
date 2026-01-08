-- Testing if sources can be referenced cross-project
-- This should fail since dbt Loom doesn't inject sources
select * from {{ source('platform_project', 'raw_customers') }}
