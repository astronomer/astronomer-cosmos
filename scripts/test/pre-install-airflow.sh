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
rm -f $AIRFLOW_HOME/airflow.cfg
rm -f $AIRFLOW_HOME/airflow.db

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
elif [ "$AIRFLOW_VERSION" = "3.2" ] ; then
  uv pip install -r requirements/requirements-airflow-3.2-dbt-1.11.txt
elif [ "$AIRFLOW_VERSION" = "3.3" ] ; then
  # TEMPORARY (Airflow 3.3.0 has not GA'd yet): install the latest 3.3 beta
  # live from the pre-release constraints instead of a pinned requirements file.
  # Replace this whole branch with a pinned
  # requirements/requirements-airflow-3.3-dbt-1.11.txt (copied from a CI
  # `pip freeze`) once 3.3.0 GAs, mirroring the 3.0-3.2 branches above.
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0b1/constraints-$PYTHON_VERSION.txt"
  curl -sSL $CONSTRAINT_URL -o /tmp/constraint.txt
  # Workaround to remove PyYAML constraint that will work on both Linux and MacOS
  sed '/PyYAML==/d' /tmp/constraint.txt > /tmp/constraint.txt.tmp
  mv /tmp/constraint.txt.tmp /tmp/constraint.txt
  # Install the beta core up front (only line needing --prerelease) so the GA
  # providers below resolve against the already-pinned 3.3.0b1.
  uv pip install --prerelease=allow "apache-airflow==3.3.0b1" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-devel-common"
  uv pip install "apache-airflow-providers-amazon[s3fs]" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-google" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-microsoft-azure" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-docker" --constraint /tmp/constraint.txt
  # DBT_VERSION isn't exported on this path; use a separate var so the dbt
  # version assertion below (which reads $DBT_VERSION) keeps its current behaviour.
  DBT_INSTALL_VERSION="${DBT_VERSION:-1.11}"
  uv pip install -U "dbt-core~=$DBT_INSTALL_VERSION" dbt-postgres dbt-bigquery dbt-vertica dbt-databricks pyspark
  uv pip install 'dbt-duckdb' "airflow-provider-duckdb>=0.2.0"
  # TEMP curation: gcsfs 2026.x requires google-cloud-storage>=3.9 (it imports the
  # google.cloud.storage.asyncio client added in 3.9). The Airflow 3.3 constraints
  # actually install a *compatible* pair (gcsfs==2026.4.0 + google-cloud-storage==
  # 3.12.0), but the unconstrained `dbt-bigquery` install above caps
  # google-cloud-storage<3.2 (dbt-bigquery requires "google-cloud-storage<3.2,>=2.4"),
  # downgrading gcs to 3.1.1 (the same version the curated 3.2 requirements file
  # lands on). gcsfs 2026.x then fails against that 3.1.1 ("Please install gcsfs to
  # access Google Storage"). gcsfs 2025.x has no such floor, so hold it to the 2025
  # line the 3.2 env uses. Run without --constraint so it isn't pinned back to the
  # constraints' 2026.4.0. Goes away with the pinned requirements-airflow-3.3-dbt-1.11.txt.
  uv pip install "gcsfs<2026"
  rm /tmp/constraint.txt
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

actual_dbt_version=$(dbt --version 2>/dev/null | awk '/Core:/{print $2}' | cut -d. -f1,2)
desired_dbt_version=$(echo "$DBT_VERSION" | cut -d. -f1,2)

if [ "$actual_dbt_version" = "$desired_dbt_version" ]; then
    echo "Version is as expected: $desired_dbt_version"
else
    echo "Version does not match. Expected: $desired_dbt_version, but got: $actual_dbt_version"
    exit 1
fi

# Installation of dbt in a separate Python virtual environment:
# Cap dbt-core below 2.0. Previously dbt-loom (installed here) held dbt-core in
# the 1.x range; with dbt-loom moved to the dedicated loom job, the bound is
# explicit so `pip install -U` doesn't pull dbt-core 2.0 (Fusion), which lacks
# the postgres adapter the subprocess tests rely on.
python -m venv venv-subprocess
venv-subprocess/bin/pip install -U "dbt-core<2.0" dbt-postgres
