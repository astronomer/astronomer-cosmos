#!/bin/bash

set -v
set -x
set -e

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
pip uninstall -y 'dbt-bigquery' 'dbt-databricks' 'dbt-duckdb' 'dbt-postgres' 'dbt-vertica' 'dbt-core'
rm -rf airflow.*
pip freeze | grep airflow
airflow db reset -y

AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

if python3 -c "import sys; print(sys.version_info >= (3, 9))" | grep -q 'True'; then
  pip install  'dbt-duckdb' 'airflow-provider-duckdb>=0.2.0'
fi

pip install 'dbt-databricks!=1.9.0' 'dbt-bigquery' 'dbt-postgres' 'dbt-vertica' 'openlineage-airflow'

# To overcome CI issues when running Py 3.10 and AF 2.6 with dbt-core 1.9
# Such as:
# ERROR tests/operators/_asynchronous/test_base.py - pydantic.errors.PydanticUserError: A non-annotated attribute was detected: `dag_id = <class 'str'>`. All model fields require a type annotation; if `dag_id` is not meant to be a field, you may be able to resolve this error by annotating it as a `ClassVar` or updating `model_config['ignored_types']`.
if [ "$AIRFLOW_VERSION" = "2.6.0" ] ; then
  pip install "pydantic<2"
  pip freeze
  pip freeze | grep -i pydantic
fi
