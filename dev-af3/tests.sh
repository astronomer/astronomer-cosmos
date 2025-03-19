#!/bin/bash

set -x

set -e

export AIRFLOW_HOME="$(pwd)/dev-af3"

airflow dags list-import-errors
