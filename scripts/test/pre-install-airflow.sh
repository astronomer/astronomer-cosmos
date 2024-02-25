#!/bin/bash

AIRFLOW_VERSION="$1"
PYTHON_VERSION="$2"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.0/constraints-$PYTHON_VERSION.txt"
curl -sSL $CONSTRAINT_URL -o /tmp/constraint.txt
# Workaround to remove PyYAML constraint that will work on both Linux and MacOS
sed '/PyYAML==/d' /tmp/constraint.txt > /tmp/constraint.txt.tmp
mv /tmp/constraint.txt.tmp /tmp/constraint.txt
# Install Airflow with constraints
pip install apache-airflow==$AIRFLOW_VERSION --constraint /tmp/constraint.txt
pip install pydantic --constraint /tmp/constraint.txt
rm /tmp/constraint.txt
