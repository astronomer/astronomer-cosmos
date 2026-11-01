#!/bin/bash

set -x
set -e

# So we can validate ExecutionMode.WATCHER_KUBERNETES
actual_version=$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))' | cut -d. -f1)
# The target test is skipped below cncf-kubernetes 10.8.0, which is only pinned on Airflow 3, so fail instead of passing without running it
if [ "$actual_version" != "3" ] ; then
    echo "This job needs Airflow 3: the test would be skipped otherwise" >&2
    exit 1
fi
pip install "apache-airflow-providers-cncf-kubernetes==10.8.0" "protobuf==6.33.2"

# Reset the Airflow database to its initial state
airflow db reset -y

# The whole file runs. jaffle_shop_kubernetes ends with a dbt docs task, which dbt-oss does not support: its test runs that task as a no-op
pytest -vv \
    -m 'integration and not dbtfusion' \
    tests/test_example_k8s_dags.py
