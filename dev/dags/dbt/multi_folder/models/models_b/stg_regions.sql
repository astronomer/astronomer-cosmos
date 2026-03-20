-- Depends on seeds: seeds_b/regions.csv and seeds_b/region_managers.csv
select
    r.id as region_id,
    r.name as region_name,
    m.manager_name
from {{ ref('regions') }} r
left join {{ ref('region_managers') }} m on r.id = m.region_id
