select
    id,
    first_name,
    this_column_does_not_exist_at_all
from {{ ref('model_a') }}
