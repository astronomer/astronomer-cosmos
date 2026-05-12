#!/bin/bash

set -x
set -e

export SOURCE_RENDERING_BEHAVIOR=all

pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check

rm -rf dbt/jaffle_shop/dbt_packages;

# When PYTEST_SPLITS and PYTEST_SPLIT_GROUP are set (CI), distribute tests across
# parallel jobs using pytest-split. When unset (local dev), all tests run as before.
SPLIT_ARGS=""
if [ -n "$PYTEST_SPLITS" ] && [ -n "$PYTEST_SPLIT_GROUP" ]; then
    SPLIT_ARGS="--splits ${PYTEST_SPLITS} --group ${PYTEST_SPLIT_GROUP} --durations-path .test_durations --splitting-algorithm least_duration"
fi

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
    --ignore=tests/test_telemetry.py \
    -k 'not (simple_dag_async or example_cosmos_python_models or example_virtualenv or jaffle_shop_kubernetes or jaffle_shop_watcher_kubernetes)' \
    $SPLIT_ARGS
