#!/bin/bash

set -v
set -x
set -e


pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install dbt-postgres==1.5.4 dbt-duckdb==1.5 dbt-databricks==1.5.4 dbt-bigquery==1.5.4
export SOURCE_RENDERING_BEHAVIOR=all
rm -rf airflow.*

AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    -m 'integration and not dbtFusion' \
    --ignore=tests/perf \
    --ignore=tests/test_example_k8s_dags.py \
    -k 'basic_cosmos_task_group'
