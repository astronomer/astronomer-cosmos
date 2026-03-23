#!/bin/bash

set -x
set -e

pytest -vv \
    --cov=cosmos \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m integration \
    tests/test_telemetry.py
