select
    id,
    first_name
from {{ ref('model_downstream') }}
