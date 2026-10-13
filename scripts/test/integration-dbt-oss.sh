#!/bin/bash

set -x
set -e
set -v

pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME
airflow db check
rm -rf dbt/jaffle_shop/dbt_packages;

# dbt-oss is the Apache 2.0 distribution of dbt v2 (https://docs.getdbt.com/blog/comparing-dbt-and-dbt-oss)
pip install "dbt-oss==2.0.5"

pytest -vv \
    tests/test_dbt_oss.py \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0
