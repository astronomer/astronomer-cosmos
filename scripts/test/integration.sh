#!/bin/bash

set -x
set -e

export SOURCE_RENDERING_BEHAVIOR=all

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
    -m 'integration and not dbtfusion' \
    --ignore=tests/perf \
    --ignore=tests/test_async_example_dag.py \
    --ignore=tests/test_example_k8s_dags.py \
    -k 'not ( example_cosmos_python_models or example_virtualenv or jaffle_shop_kubernetes)'
