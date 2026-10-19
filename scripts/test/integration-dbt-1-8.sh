#!/bin/bash

set -v
set -x
set -e


DBT_VERSION="${1:-1.8}"

pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
# dbt-postgres and dbt-duckdb have no 1.12 release, so only 1.8 and 1.9 pin the adapters to the dbt minor.
if [ "$DBT_VERSION" = "1.8" ] || [ "$DBT_VERSION" = "1.9" ]; then
    ADAPTER_SPEC="~=$DBT_VERSION.0"
else
    ADAPTER_SPEC=""
fi
pip install "dbt-core~=$DBT_VERSION.0" "dbt-postgres$ADAPTER_SPEC" "dbt-duckdb$ADAPTER_SPEC" "dbt-databricks$ADAPTER_SPEC" "dbt-bigquery$ADAPTER_SPEC"

# dbt 1.9 adapters can pull in pydantic 1.10.x, which breaks apache-airflow-core 3. Use a floor, not
# an exact pin, because newer Airflow needs pydantic>2.11.0 (FieldInfo._copy) and a pin downgrades it.
if [ "$DBT_VERSION" = "1.9" ]; then
    pip install "pydantic>2.11.0"
fi

actual_dbt_version=$(dbt --version | awk '/installed:/ { split($3, v, "."); print v[1]"."v[2] }')
if [ "$actual_dbt_version" != "$DBT_VERSION" ]; then
    echo "Version does not match. Expected: $DBT_VERSION, but got: $actual_dbt_version"
    exit 1
fi
export SOURCE_RENDERING_BEHAVIOR=all
rm -rf airflow.*

AIRFLOW_VERSION=$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))')
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    -m 'integration and not dbtFusion' \
    --ignore=tests/perf \
    --ignore=tests/test_example_k8s_dags.py \
    "tests/test_example_dags.py::test_example_dag[basic_cosmos_task_group]"
