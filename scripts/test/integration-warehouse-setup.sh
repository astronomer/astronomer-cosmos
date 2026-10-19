#!/bin/bash

set -x
set -e

AIRFLOW_VERSION="$1"
DBT_VERSION="$2"
LOCKFILE="requirements/requirements-airflow-${AIRFLOW_VERSION}-dbt-${DBT_VERSION}.txt"
# dbt-databricks >= 1.12.5 (metric views) and dbt-snowflake (semantic views) need these lower than the lockfiles pin them.
RELAXED_PINS="certifi|databricks-sdk|dbt-databricks|packaging"
CONSTRAINTS_FILE=$(mktemp)

grep -v -i -E "^($RELAXED_PINS)==" "$LOCKFILE" > "$CONSTRAINTS_FILE"
pip install -c "$CONSTRAINTS_FILE" "dbt-databricks>=1.12.5,<1.13" "dbt-snowflake>=1.12,<1.13"

# The hatch test tools already conflict with the lockfile (e.g. mypy needs pathspec>=1.0, dbt-core <1.1), so this only reports.
pip check || true
