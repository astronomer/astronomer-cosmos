#!/bin/bash

set -x

set -e

export AIRFLOW_HOME="$(pwd)/dev-af3"

# Print out the AIRFLOW_HOME value for debugging purposes (optional)
echo "AIRFLOW_HOME is set to $AIRFLOW_HOME"

# Run Airflow in standalone mode
airflow standalone
