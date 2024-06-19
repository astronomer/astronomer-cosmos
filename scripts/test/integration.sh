#!/bin/bash

set -x
set -e

pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check


rm -rf dbt/jaffle_shop/dbt_packages;
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m integration  \
    --ignore=tests/perf \
    -k 'not (sqlite or example_cosmos_sources or example_cosmos_python_models or example_virtualenv)'
