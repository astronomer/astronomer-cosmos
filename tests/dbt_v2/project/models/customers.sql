select
    id,
    concat(first_name, ' ', last_name) as full_name
from {{ ref('stg_customers') }}
