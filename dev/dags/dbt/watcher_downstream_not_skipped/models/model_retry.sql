{{
    config(
        pre_hook=[
            "DO $$ BEGIN IF nextval('{{ target.schema }}._cosmos_fail_once_seq') <= 1 THEN RAISE EXCEPTION 'fail_once: intentional first-run failure'; END IF; END $$"
        ]
    )
}}

select
    id,
    first_name
from {{ ref('model_a') }}
