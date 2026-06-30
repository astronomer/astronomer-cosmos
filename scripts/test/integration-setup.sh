#!/bin/bash

set -v
set -x
set -e

DBT_VERSION="$1"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

# dbt Core 2.0 (the open-source build of the dbt Fusion engine) is alpha-only today and is
# NOT installed by pre-install-airflow.sh, which pins the dbt 1.11 baseline. For 2.x matrix
# cells, (re)install dbt-core to the 2.x line allowing pre-releases. The lower bound uses an
# explicit "a1" pre-release tag because PEP 440 sorts 2.0.0a3 < 2.0.0, so a bare ">=2.0" would
# exclude the alpha. Postgres is the integration adapter; only its 1.11 beta resolves with 2.0.
case "$DBT_VERSION" in
  2.*)
    uv pip uninstall dbt-core dbt-postgres dbt-adapters dbt-common dbt-extractor dbt-semantic-interfaces || true
    uv pip install -U --prerelease=allow "dbt-core>=${DBT_VERSION}a1,<$NEXT_MINOR_VERSION" dbt-postgres
    ;;
esac

# we install using the following workaround to overcome installation conflicts, such as:
# apache-airflow 2.3.0 and dbt-core [0.13.0 - 1.5.2] and jinja2>=3.0.0 because these package versions have conflicting dependencies
rm -f $AIRFLOW_HOME/airflow.cfg
rm -f $AIRFLOW_HOME/airflow.db

airflow db reset -y

# ATTENTION: Airflow and its dependencies are installed using pre-install-airflow.sh script, so we don't install them here.
AIRFLOW_VERSION=$(airflow version | tr -d '[:space:]')
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | grep -oE '^[0-9]+')

echo "Detected Airflow version: [$AIRFLOW_VERSION]"

if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi

echo "Packages installed:"
uv pip freeze
