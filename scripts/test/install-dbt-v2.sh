#!/bin/bash

set -x
set -e

# Install dbt-oss, the Apache 2.0 distribution of dbt v2 (https://docs.getdbt.com/blog/comparing-dbt-and-dbt-oss),
# for a hatch ``dbt`` matrix value: ``v<major>-oss`` (such as ``v2-oss``, the latest dbt-oss 2.x) or
# ``v<major>.<minor>-oss`` (such as ``v2.0-oss``, the latest dbt-oss 2.0.x).
DBT_VERSION="$1"

case "$DBT_VERSION" in
  v*-oss)
    SERIES="${DBT_VERSION#v}"
    SERIES="${SERIES%-oss}"
    ;;
  *)
    echo "::error::Unsupported dbt-oss version '$DBT_VERSION'. Expected v<major>-oss or v<major>.<minor>-oss."
    exit 1
    ;;
esac

# Hatch runs this script with sh, which may be dash, so keep it POSIX: no [[ ]].
case "$SERIES" in
  *.*) DBT_OSS_SPEC="dbt-oss~=${SERIES}.0" ;;
  *) DBT_OSS_SPEC="dbt-oss>=${SERIES},<$((SERIES + 1))" ;;
esac

# dbt-oss and dbt-core 1.x both provide the ``dbt`` Python package, so remove dbt 1.x and its adapters first.
pip uninstall -y 'dbt-bigquery' 'dbt-databricks' 'dbt-duckdb' 'dbt-postgres' 'dbt-vertica' 'dbt-core'
pip install "$DBT_OSS_SPEC"

dbt --version | grep -q "^dbt-oss ${SERIES}\." || { echo "::error::Expected dbt-oss ${SERIES}.x to be installed"; exit 1; }
