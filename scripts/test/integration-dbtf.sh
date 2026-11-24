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


# Without --version, the installer picks the latest stable dbt Fusion release.

curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update

pytest -vv \
    tests/test_dbtf.py \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0
