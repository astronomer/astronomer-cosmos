#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

pip uninstall -y 'dbt-bigquery' 'dbt-databricks' 'dbt-duckdb' 'dbt-postgres' 'dbt-vertica' 'dbt-core'
rm -rf airflow.*
pip freeze | grep airflow
airflow db reset -y

AIRFLOW_VERSION=$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))')
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    uv pip install cadwyn!=5.4.0
fi
echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
airflow db migrate

uv pip freeze
