#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION: $DBT_VERSION"

# Calculate next minor version (e.g. "1.11" -> "1.12")
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

# Runs the cross-project example DAG that exercises dbt-loom. Carved out of
# the main integration suite so the loom-specific install and the loom job's
# dbt version can evolve independently of the main matrix.

echo "Pinning dbt-core to $DBT_VERSION + installing dbt-loom and dbt-postgres in the main env"
pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y || true
pip install -U "dbt-core>=$DBT_VERSION,<$NEXT_MINOR_VERSION" dbt-loom dbt-postgres

# cross_project_manifest_dag.py runs dbt subprocesses via venv-subprocess/.
# Ensure that env also has dbt-loom for the cross-project reference path.
if [ -d "venv-subprocess" ]; then
    echo "Installing dbt-loom and dbt-postgres in venv-subprocess/"
    venv-subprocess/bin/pip install -U "dbt-core>=$DBT_VERSION,<$NEXT_MINOR_VERSION" dbt-loom dbt-postgres
fi

actual_dbt_version=$(dbt --version | awk '/installed:/ { split($3, v, "."); print v[1]"."v[2] }')
if [ "$actual_dbt_version" = "$DBT_VERSION" ]; then
    echo "Version is as expected: $DBT_VERSION"
else
    echo "Version does not match. Expected: $DBT_VERSION, but got: $actual_dbt_version"
    exit 1
fi

# Reset Airflow state before running the DAG
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

# Run only the cross-project example DAG that exercises dbt-loom
pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    "tests/test_example_dags.py::test_example_dag[cross_project_manifest_dag]"
