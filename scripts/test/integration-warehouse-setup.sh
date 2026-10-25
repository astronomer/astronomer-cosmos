#!/bin/bash

set -x
set -e

AIRFLOW_VERSION="$1"
DBT_VERSION="$2"
# dbt-oss has no lockfile of its own: its environment starts from the dbt 1.12 one (see pre-install-airflow.sh).
case "$DBT_VERSION" in
  *-dbt-oss) LOCKFILE_DBT_VERSION="1.12" ;;
  *) LOCKFILE_DBT_VERSION="$DBT_VERSION" ;;
esac
LOCKFILE="requirements/requirements-airflow-${AIRFLOW_VERSION}-dbt-${LOCKFILE_DBT_VERSION}.txt"
# dbt-databricks >= 1.12.5 (metric views) and dbt-snowflake (semantic views) need these off the lockfile pins:
# certifi and packaging lower, databricks-sdk and dbt-databricks higher.
RELAXED_PINS="certifi|databricks-sdk|dbt-databricks|packaging"
CONSTRAINTS_FILE=$(mktemp)

grep -v -i -E "^($RELAXED_PINS)==" "$LOCKFILE" > "$CONSTRAINTS_FILE"

case "$DBT_VERSION" in
  *-dbt-oss)
    # dbt-oss has the Databricks and Snowflake adapters built in, and dbt-databricks and dbt-snowflake would provide the
    # `dbt` package again, so install dbt-oss and only the clients the tests call directly (the adapters bring them on
    # dbt 1.x).
    sh scripts/test/install-dbt-oss.sh "$DBT_VERSION"
    pip install -c "$CONSTRAINTS_FILE" databricks-sql-connector snowflake-connector-python
    ;;
  *)
    pip install -c "$CONSTRAINTS_FILE" "dbt-databricks>=1.12.5,<1.13" "dbt-snowflake>=1.12,<1.13"
    ;;
esac

# The lockfile constraints move idna and pathspec back to its pins, below what httpx2 and mypy in the hatch env need, so this only reports.
pip check || true
