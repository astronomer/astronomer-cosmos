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

# Install dbt fusion, pinned: the default latest (2.0.0-preview.209) hangs on the BigQuery test
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update --version 2.0.0-preview.205

pytest -vv \
    tests/test_dbtf.py \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0
