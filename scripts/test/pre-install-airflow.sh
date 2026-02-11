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

# Remove artifacts used locally
rm -f "${AIRFLOW_HOME:?}/airflow.cfg"
rm -f "${AIRFLOW_HOME:?}/airflow.db"

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
  # Download Airflow constraints according to the version being used
  if [ "$AIRFLOW_VERSION" = "3.0" ] ; then
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.2/constraints-$PYTHON_VERSION.txt"
  elif [ "$AIRFLOW_VERSION" = "3.1" ] ; then
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.0/constraints-$PYTHON_VERSION.txt"
    uv pip install "apache-airflow-devel-common"
  else
    CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.0/constraints-$PYTHON_VERSION.txt"
  fi;

  curl -sSL $CONSTRAINT_URL -o /tmp/constraint.txt
  # Workaround to remove PyYAML constraint that will work on both Linux and MacOS
  sed '/PyYAML==/d' /tmp/constraint.txt > /tmp/constraint.txt.tmp

  mv /tmp/constraint.txt.tmp /tmp/constraint.txt

  # Install Airflow providers, dbt adapters
  uv pip install "apache-airflow-providers-amazon[s3fs]" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-google" "apache-airflow==$AIRFLOW_VERSION" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-microsoft-azure" --constraint /tmp/constraint.txt
  uv pip install -U "dbt-core~=$DBT_VERSION" dbt-postgres dbt-bigquery dbt-vertica dbt-databricks pyspark
  uv pip install 'dbt-duckdb' "airflow-provider-duckdb>=0.2.0" apache-airflow==$AIRFLOW_VERSION
  uv pip install dbt-loom

  # Delete the no longer needed constraint file
  rm /tmp/constraint.txt
fi

actual_airflow_version=$(airflow version 2>/dev/null | tail -1 | cut -d. -f1,2)
desired_airflow_version=$(echo $AIRFLOW_VERSION | cut -d. -f1,2)

if [ "$actual_airflow_version" = $desired_airflow_version ]; then
    echo "Version is as expected: $desired_airflow_version"
else
    echo "Version does not match. Expected: $desired_airflow_version, but got: $actual_airflow_version"
    exit 1
fi

actual_dbt_version=$(dbt --version 2>/dev/null | awk '/Core:/{print $2}' | cut -d. -f1,2)
desired_dbt_version=$(echo "$DBT_VERSION" | cut -d. -f1,2)

if [ "$actual_dbt_version" = "$desired_dbt_version" ]; then
    echo "Version is as expected: $desired_dbt_version"
else
    echo "Version does not match. Expected: $desired_dbt_version, but got: $actual_dbt_version"
    exit 1
fi

# Installation of dbt in a separate Python virtual environment:
python -m venv venv-subprocess
venv-subprocess/bin/pip install -U dbt-postgres dbt-loom
