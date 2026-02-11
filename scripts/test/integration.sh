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
    --ignore=tests/operators/test_watcher_kubernetes_integration.py \
    --ignore=dev/dags/cross_project_dbt_ls_dag.py \
    -k 'not (simple_dag_async or example_cosmos_python_models or example_virtualenv or jaffle_shop_kubernetes or jaffle_shop_watcher_kubernetes)'
