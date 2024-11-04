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

if [ "$AIRFLOW_VERSION" = "2.4" ] ; then
  uv pip install "apache-airflow-providers-amazon[s3fs]" \
    "apache-airflow-providers-cncf-kubernetes" \
    "apache-airflow-providers-google<10.11." \
    "apache-airflow-providers-microsoft-azure"
  uv pip install pyopenssl --upgrade
else
  uv pip install "apache-airflow-providers-amazon[s3fs]>=3.0.0" # --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes>=5.1.1" # --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-google>=10.11.0" # --constraint /tmp/constraint.txt
  uv pip install apache-airflow-providers-microsoft-azure # --constraint /tmp/constraint.txt
fi

rm /tmp/constraint.txt
