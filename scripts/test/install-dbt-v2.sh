#!/bin/bash

set -x
set -e

# Install dbt v2 for a hatch ``dbt`` matrix value such as ``v2.0-oss`` (``dbt-oss``, the Apache 2.0
# distribution) or ``v2.0`` (``dbt``). See https://docs.getdbt.com/blog/comparing-dbt-and-dbt-oss
DBT_VERSION="$1"

case "$DBT_VERSION" in
  v*-oss)
    PACKAGE="dbt-oss"
    MINOR="${DBT_VERSION#v}"
    MINOR="${MINOR%-oss}"
    ;;
  v*)
    PACKAGE="dbt"
    MINOR="${DBT_VERSION#v}"
    ;;
  *)
    echo "::error::Unsupported dbt v2 version '$DBT_VERSION'. Expected v<major>.<minor> or v<major>.<minor>-oss."
    exit 1
    ;;
esac

# dbt v2 and dbt-core 1.x both provide the ``dbt`` Python package, so remove dbt 1.x and its adapters first.
pip uninstall -y 'dbt-bigquery' 'dbt-databricks' 'dbt-duckdb' 'dbt-postgres' 'dbt-vertica' 'dbt-core'
pip install "${PACKAGE}~=${MINOR}.0"

dbt --version | grep -q "^${PACKAGE} ${MINOR}\." || { echo "::error::Expected ${PACKAGE} ${MINOR}.x to be installed"; exit 1; }
