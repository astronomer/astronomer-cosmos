#!/bin/bash

set -x
set -e

airflow db check

# TEMP, revert before merge: -s and log_cli show the logs of the DAG runs, which pytest hides for tests that pass.
pytest -vv \
    --durations=0 \
    -rs \
    --timeout=1800 \
    --timeout-method=thread \
    -s \
    -o log_cli=true \
    -o log_cli_level=DEBUG \
    -m 'integration' \
    tests/test_warehouse_metric_and_semantic_views.py \
    "$@"
