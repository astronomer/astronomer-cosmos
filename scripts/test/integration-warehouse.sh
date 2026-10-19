#!/bin/bash

set -x
set -e

airflow db check

pytest -vv \
    --durations=0 \
    --timeout=1800 \
    --timeout-method=thread \
    -m 'integration' \
    tests/test_warehouse_metric_and_semantic_views.py \
    "$@"
