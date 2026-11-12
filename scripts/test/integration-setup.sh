#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

rm -f $AIRFLOW_HOME/airflow.cfg
rm -f $AIRFLOW_HOME/airflow.db

airflow db reset -y

# ATTENTION: Airflow and its dependencies are installed using pre-install-airflow.sh script, so we don't install them here.
AIRFLOW_VERSION=$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))')

echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
airflow db migrate

echo "Packages installed:"
uv pip freeze
