#!/bin/bash

set -x
set -e

AIRFLOW_VERSION="$1"
DBT_VERSION="$2"
LOCKFILE="requirements/requirements-airflow-${AIRFLOW_VERSION}-dbt-${DBT_VERSION}.txt"
# dbt-databricks >= 1.12.5 (metric views) and dbt-snowflake (semantic views) need these off the lockfile pins:
# certifi and packaging lower, databricks-sdk and dbt-databricks higher.
RELAXED_PINS="certifi|databricks-sdk|dbt-databricks|packaging"
CONSTRAINTS_FILE=$(mktemp)

grep -v -i -E "^($RELAXED_PINS)==" "$LOCKFILE" > "$CONSTRAINTS_FILE"
pip install -c "$CONSTRAINTS_FILE" "dbt-databricks>=1.12.5,<1.13" "dbt-snowflake>=1.12,<1.13"

# The lockfile constraints move idna and pathspec back to its pins, below what httpx2 and mypy in the hatch env need, so this only reports.
pip check || true
