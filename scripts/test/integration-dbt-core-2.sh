#!/bin/bash

set -x
set -e
set -v

pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME
airflow db check
rm -rf dbt/jaffle_shop/dbt_packages;

# dbt-core 2.0 is the Rust engine; the wheel ships the `dbt` binary and a Python `dbt` package without the
# 1.x API (no dbt.version, no dbtRunner callbacks), so Cosmos drives it through InvocationMode.SUBPROCESS.
# Pinned to a release candidate until 2.0.0 is final; bump deliberately.
uv pip install "dbt-core==2.0.0rc2"
dbt --version

pytest -vv \
    tests/test_dbt_core_2.py \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0
