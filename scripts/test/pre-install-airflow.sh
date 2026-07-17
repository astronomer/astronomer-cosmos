#!/bin/bash

set -v
set -x
set -e

AIRFLOW_VERSION="$1"
PYTHON_VERSION="$2"
# dbt minor from Hatch's {matrix:dbt}; required, no default.
DBT_VERSION="$3"
if [ -z "$DBT_VERSION" ]; then
  echo "::error::DBT_VERSION (third positional arg) was not provided."
  exit 1
fi

# Use this to set the appropriate Python environment in Github Actions,
# while also not assuming --system when running locally.
if [ "$GITHUB_ACTIONS" = "true" ] && [ -z "${VIRTUAL_ENV}" ]; then
  py_path=$(which python)
  virtual_env_dir=$(dirname "$(dirname "$py_path")")
  export VIRTUAL_ENV="$virtual_env_dir"
  echo "${VIRTUAL_ENV}"
fi

# Remove artifacts used locally
rm -f $AIRFLOW_HOME/airflow.cfg
rm -f $AIRFLOW_HOME/airflow.db

# Install Airflow with constraints
pip install uv
uv pip install pip --upgrade

EFFECTIVE_DBT_VERSION="$DBT_VERSION"

if [ "$AIRFLOW_VERSION" = "2.9" ] || [ "$AIRFLOW_VERSION" = "2.10" ] || [ "$AIRFLOW_VERSION" = "2.11" ] || [ "$AIRFLOW_VERSION" = "3.0" ] || [ "$AIRFLOW_VERSION" = "3.1" ] || [ "$AIRFLOW_VERSION" = "3.2" ] || [ "$AIRFLOW_VERSION" = "3.3" ] ; then
  # Install from the pinned lockfile matching this (airflow, dbt) pair.
  REQUIREMENTS_FILE="requirements/requirements-airflow-${AIRFLOW_VERSION}-dbt-${DBT_VERSION}.txt"
  if [ ! -f "$REQUIREMENTS_FILE" ]; then
    if [ "$DBT_VERSION" = "1.11" ] || [ "$DBT_VERSION" = "1.12" ]; then
      # Every Airflow version should have both a 1.11 and 1.12 lockfile; a missing
      # one is a real gap, not an intentionally-uncovered dbt minor.
      echo "::error::No pinned lockfile for airflow $AIRFLOW_VERSION + dbt $DBT_VERSION ($REQUIREMENTS_FILE). Add one."
      exit 1
    fi
    # Other dbt minors (1.5-1.10, 2.0) have no per-Airflow lockfile; jobs needing
    # them re-pin dbt in their own setup step, so this is just a throwaway baseline.
    echo "::warning::No lockfile for airflow $AIRFLOW_VERSION + dbt $DBT_VERSION; falling back to the dbt-1.11 baseline (this job must re-pin dbt itself)."
    REQUIREMENTS_FILE="requirements/requirements-airflow-${AIRFLOW_VERSION}-dbt-1.11.txt"
    EFFECTIVE_DBT_VERSION="1.11"
  fi
  echo "Installing pinned requirements from $REQUIREMENTS_FILE"
  uv pip install -r "$REQUIREMENTS_FILE"
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

# EFFECTIVE_DBT_VERSION differs from DBT_VERSION only when the fallback above
# substituted the dbt-1.11 baseline.
actual_dbt_version=$(dbt --version 2>/dev/null | awk '/installed:/ { split($3, v, "."); print v[1]"."v[2] }')
if [ "$actual_dbt_version" = "$EFFECTIVE_DBT_VERSION" ]; then
    echo "dbt version is as expected: $EFFECTIVE_DBT_VERSION"
else
    echo "dbt version does not match. Expected: $EFFECTIVE_DBT_VERSION, but got: $actual_dbt_version"
    exit 1
fi

# Installation of dbt in a separate Python virtual environment:
# Cap dbt-core below 2.0. Previously dbt-loom (installed here) held dbt-core in
# the 1.x range; with dbt-loom moved to the dedicated loom job, the bound is
# explicit so `pip install -U` doesn't pull dbt-core 2.0 (Fusion), which lacks
# the postgres adapter the subprocess tests rely on.
python -m venv venv-subprocess
venv-subprocess/bin/pip install -U "dbt-core<2.0" dbt-postgres
