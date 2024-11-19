#!/bin/bash

set -x
set -v

AIRFLOW_VERSION="$1"
PYTHON_VERSION="$2"

# Use this to set the appropriate Python environment in Github Actions,
# while also not assuming --system when running locally.
if [ "$GITHUB_ACTIONS" = "true" ] && [ -z "${VIRTUAL_ENV}" ]; then
  py_path=$(which python)
  virtual_env_dir=$(dirname "$(dirname "$py_path")")
  export VIRTUAL_ENV="$virtual_env_dir"
fi

echo "${VIRTUAL_ENV}"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.0/constraints-$PYTHON_VERSION.txt"
curl -sSL $CONSTRAINT_URL -o /tmp/constraint.txt
# Workaround to remove PyYAML constraint that will work on both Linux and MacOS
sed '/PyYAML==/d' /tmp/constraint.txt > /tmp/constraint.txt.tmp
mv /tmp/constraint.txt.tmp /tmp/constraint.txt

# Install Airflow with constraints
pip install uv
uv pip install pip --upgrade

uv pip install "apache-airflow==$AIRFLOW_VERSION" --constraint /tmp/constraint.txt
uv pip install apache-airflow-providers-docker --constraint /tmp/constraint.txt
uv pip install apache-airflow-providers-postgres --constraint /tmp/constraint.txt

if [ "$AIRFLOW_VERSION" = "2.4" ] || [ "$AIRFLOW_VERSION" = "2.5" ] || [ "$AIRFLOW_VERSION" = "2.6" ]  ; then
  uv pip install "apache-airflow-providers-amazon" "apache-airflow==$AIRFLOW_VERSION" "urllib3<2"
  uv pip install "apache-airflow-providers-cncf-kubernetes" "apache-airflow==$AIRFLOW_VERSION"
  uv pip install  "apache-airflow-providers-google<10.11" "apache-airflow==$AIRFLOW_VERSION"
  uv pip install "apache-airflow-providers-microsoft-azure" "apache-airflow==$AIRFLOW_VERSION"
  uv pip install pyopenssl --upgrade
elif [ "$AIRFLOW_VERSION" = "2.7" ] ; then
  uv pip install "apache-airflow-providers-amazon" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
  uv pip install  "apache-airflow-providers-google>10.11" "apache-airflow==$AIRFLOW_VERSION"
  uv pip install apache-airflow-providers-microsoft-azure --constraint /tmp/constraint.txt
else
  uv pip install "apache-airflow-providers-amazon[s3fs]" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
  # The Airflow 2.9 constraints file at
  # https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.11.txt
  # specifies apache-airflow-providers-google==10.16.0. However, our CI setup uses a Google connection without a token,
  # which previously led to authentication issues when the token was None. This issue was resolved in PR
  # https://github.com/apache/airflow/pull/38102 and fixed in apache-airflow-providers-google==10.17.0. Consequently,
  # we are using apache-airflow-providers-google>=10.17.0 and skipping constraints installation, as the specified
  # version does not meet our requirements.
  uv pip install "apache-airflow-providers-google>=10.17.0"
  uv pip install apache-airflow-providers-microsoft-azure --constraint /tmp/constraint.txt
fi

rm /tmp/constraint.txt

actual_version=$(airflow version | cut -d. -f1,2)

if [ "$actual_version" = $AIRFLOW_VERSION ]; then
    echo "Version is as expected: $AIRFLOW_VERSION"
else
    echo "Version does not match. Expected: $AIRFLOW_VERSION, but got: $actual_version"
    exit 1
fi
