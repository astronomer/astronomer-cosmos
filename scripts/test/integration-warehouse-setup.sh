#!/bin/bash

set -x
set -e

AIRFLOW_VERSION="$1"
DBT_VERSION="$2"
# TEMP, REVERT BEFORE MERGE: pull_request_target runs the workflow file from main, so the new 2.0-dbt-oss matrix cell
# does not run on this PR. Force the existing dbt 1.12 cell to install dbt-oss so the tests can run against it in CI.
DBT_VERSION="2.0-dbt-oss"
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
    # None of the dbt 1.x packages that provide the `dbt` package may be left next to dbt-oss, in particular not the
    # Databricks and Snowflake adapters, or the job would not be testing dbt-oss. This matches the packages that
    # install-dbt-oss.sh uninstalls (dbt-state is not in that list, but requires dbt-core).
    if pip freeze | grep -i -E '^dbt[-_](core|adapters|common|databricks|snowflake|spark|loom|state|extractor|protos|semantic[-_]interfaces)=='; then
      echo "::error::A dbt 1.x package is installed next to dbt-oss"
      exit 1
    fi
    ;;
  *)
    pip install -c "$CONSTRAINTS_FILE" "dbt-databricks>=1.12.5,<1.13" "dbt-snowflake>=1.12,<1.13"
    ;;
esac

# The lockfile constraints move idna and pathspec back to its pins, below what httpx2 and mypy in the hatch env need, so this only reports.
pip check || true
