#!/bin/bash

set -x
set -e

airflow db check

pytest -vv \
    --durations=0 \
    --timeout=1800 \
    --timeout-method=thread \
    -m 'warehouse_e2e' \
    tests/e2e \
    "$@"
