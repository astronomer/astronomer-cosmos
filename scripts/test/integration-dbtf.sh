#!/bin/bash

set -x
set -e
set -v

export SOURCE_RENDERING_BEHAVIOR=all
pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME
airflow db check
rm -rf dbt/jaffle_shop/dbt_packages;


# Note: the dbt Fusion Engine is in Beta! Bugs and missing functionality compared to dbt Core will be resolved
# continuously in the lead-up to a final release (see more details in https://github.com/dbt-labs/dbt-fusion)

# Install dbt fusion (2.0.0-beta.26 on 23 June 2025)
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update

pytest -vv \
    tests/test_dbtf.py \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0
