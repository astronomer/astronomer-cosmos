-- Intentionally broken model: references a nonexistent column
select
    region_id,
    region_name,
    this_column_does_not_exist
from {{ ref('stg_regions') }}
