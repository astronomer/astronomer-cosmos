{{
    config(
        pre_hook=[
            "{{ fail_once('_cosmos_fail_once') }}"
        ]
    )
}}

select
    id,
    first_name
from {{ ref('model_a') }}
