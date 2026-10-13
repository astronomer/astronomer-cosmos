#!/bin/bash

set -x
set -e
set -v

pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME
airflow db check
rm -rf dbt/jaffle_shop/dbt_packages;

# dbt v2 does not support the Postgres adapter the other integration tests use, so only the duckdb tests run.
pytest -vv \
    tests/test_dbt_oss.py \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0
