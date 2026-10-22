#!/bin/bash

set -x
set -e

# The hatch ``dbt`` matrix value ``2.0-dbt-oss`` installs the latest dbt-oss 2.0.x.
DBT_VERSION="$1"

case "$DBT_VERSION" in
  *-dbt-oss)
    MINOR="${DBT_VERSION%-dbt-oss}"
    ;;
  *)
    echo "::error::Unsupported dbt-oss version '$DBT_VERSION'. Expected <major>.<minor>-dbt-oss."
    exit 1
    ;;
esac

# dbt-oss and dbt-core 1.x both provide the ``dbt`` Python package, so remove dbt 1.x and its adapters first, together
# with the packages only dbt-core depends on. Leaving ``dbt-adapters`` behind would make ``dbt.adapters.factory``
# importable here, but not on a clean dbt-oss install.
pip uninstall -y 'dbt-bigquery' 'dbt-databricks' 'dbt-duckdb' 'dbt-postgres' 'dbt-spark' 'dbt-vertica' 'dbt-core' \
  'dbt-adapters' 'dbt-common' 'dbt-extractor' 'dbt-protos' 'dbt-semantic-interfaces' 'dbt-core-experimental-parser'
pip install "dbt-oss~=${MINOR}.0"

dbt --version | grep -q "^dbt-oss ${MINOR}\." || { echo "::error::Expected dbt-oss ${MINOR}.x to be installed"; exit 1; }
pip freeze | grep -i '^dbt'
