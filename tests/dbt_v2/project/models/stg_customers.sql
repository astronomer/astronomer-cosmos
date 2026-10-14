select
    id,
    first_name,
    last_name
from {{ ref('raw_customers') }}
