#!/bin/bash

set -v
set -x
set -e

AIRFLOW_VERSION="$1"
PYTHON_VERSION="$2"

# Use this to set the appropriate Python environment in Github Actions,
# while also not assuming --system when running locally.
if [ "$GITHUB_ACTIONS" = "true" ] && [ -z "${VIRTUAL_ENV}" ]; then
  py_path=$(which python)
  virtual_env_dir=$(dirname "$(dirname "$py_path")")
  export VIRTUAL_ENV="$virtual_env_dir"
  echo "${VIRTUAL_ENV}"
fi

# Install Airflow with constraints
pip install uv
uv pip install pip --upgrade

if [ "$AIRFLOW_VERSION" = "2.9" ] ; then
  uv pip install -r requirements/requirements-airflow-2.9-dbt-1.11.txt
elif [ "$AIRFLOW_VERSION" = "2.10" ] ; then
  uv pip install -r requirements/requirements-airflow-2.10-dbt-1.11.txt
elif [ "$AIRFLOW_VERSION" = "2.11" ] ; then
  uv pip install -r requirements/requirements-airflow-2.11-dbt-1.11.txt
elif [ "$AIRFLOW_VERSION" = "3.0" ] ; then
  uv pip install -r requirements/requirements-airflow-3.0-dbt-1.11.txt
elif [ "$AIRFLOW_VERSION" = "3.1" ] ; then
  uv pip install -r requirements/requirements-airflow-3.1-dbt-1.11.txt
else
  uv pip install "apache-airflow-providers-amazon[s3fs]" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-google" "apache-airflow==$AIRFLOW_VERSION" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-microsoft-azure" --constraint /tmp/constraint.txt
  uv pip install -U "dbt-core~=$DBT_VERSION" dbt-postgres dbt-bigquery dbt-vertica dbt-databricks pyspark
  uv pip install 'dbt-duckdb' "airflow-provider-duckdb>=0.2.0" apache-airflow==$AIRFLOW_VERSION
  uv pip install dbt-loom
fi

actual_version=$(airflow version 2>/dev/null | tail -1 | cut -d. -f1,2)
desired_version=$(echo $AIRFLOW_VERSION | cut -d. -f1,2)

if [ "$actual_version" = $desired_version ]; then
    echo "Version is as expected: $desired_version"
else
    echo "Version does not match. Expected: $desired_version, but got: $actual_version"
    exit 1
fi

# Installation of dbt in a separate Python virtual environment:
python -m venv venv-subprocess
venv-subprocess/bin/pip install -U dbt-postgres
