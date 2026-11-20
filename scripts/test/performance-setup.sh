#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION:"
echo "$DBT_VERSION"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install -U \
  "dbt-core>=$DBT_VERSION,<$NEXT_MINOR_VERSION" dbt-postgres

rm -rf airflow.*

AIRFLOW_VERSION=$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))')
echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
airflow db migrate
