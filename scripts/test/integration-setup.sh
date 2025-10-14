#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
pip uninstall -y 'dbt-bigquery' 'dbt-duckdb' 'dbt-databricks' 'dbt-postgres' 'dbt-vertica' 'dbt-core'

rm -f $AIRFLOW_HOME/airflow.cfg
rm -f $AIRFLOW_HOME/airflow.db

pip freeze | grep airflow
airflow db reset -y


AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
  # https://github.com/zmievsa/cadwyn/issues/283
    uv pip install "cadwyn>=5.4.1"
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

uv pip install -U "dbt-core~=$DBT_VERSION" dbt-postgres dbt-bigquery dbt-vertica dbt-databricks pyspark  "apache-airflow==$AIRFLOW_VERSION"

# The DuckDB adaptor has not been actively maintained and its dependencies conflict with other latest dbt adapters and Airflow.
# For this reason, we're installing it in a separate Python virtualenv.
# Example of error we were getting before this isolationw as introduced:
# dbt is raising No module named 'dbt.adapters.catalogs'
if python3 -c "import sys; print(sys.version_info >= (3, 9))" | grep -q 'True'; then
  pip install 'airflow-provider-duckdb>=0.2.0'
  python -m venv /tmp/venv-duckdb
  /bin/bash -c ". /tmp/venv-duckdb/bin/activate; pip install install dbt-duckdb; deactivate"
fi

# To overcome CI issues when running Py 3.10 and AF 2.6 with dbt-core 1.9
# Such as:
# ERROR tests/operators/_asynchronous/test_base.py - pydantic.errors.PydanticUserError: A non-annotated attribute was detected: `dag_id = <class 'str'>`. All model fields require a type annotation; if `dag_id` is not meant to be a field, you may be able to resolve this error by annotating it as a `ClassVar` or updating `model_config['ignored_types']`.
if [ "$AIRFLOW_VERSION" = "2.6.0" ] ; then
  pip install "pydantic<2"
  pip freeze
  pip freeze | grep -i pydantic
fi

pip install -U openlineage-airflow apache-airflow==$AIRFLOW_VERSION

uv pip freeze
