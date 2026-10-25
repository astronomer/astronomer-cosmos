#!/bin/bash

set -x
set -e

# So we can validate ExecutionMode.WATCHER_KUBERNETES
actual_version=$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))' | cut -d. -f1)
if [ "$actual_version" = "3" ] ; then
    pip install "apache-airflow-providers-cncf-kubernetes==10.8.0" "protobuf==6.33.2"
fi

# Reset the Airflow database to its initial state
airflow db reset -y

# Only the watcher example runs here: the other one (jaffle_shop_kubernetes) ends with a dbt docs task, which dbt-oss does not support
pytest -vv \
    -m 'integration and not dbtfusion' \
    -k test_example_dag_watcher_kubernetes \
    tests/test_example_k8s_dags.py
