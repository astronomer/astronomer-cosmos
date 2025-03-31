#!/bin/bash

# Exit on error
set -e

# Create a UV virtual environment named 'env' (you can change this as needed)
echo "Creating UV virtual environment at $(pwd)/tools"
uv venv venv --directory "$(pwd)/scripts/airflow3"

# Activate the virtual environment
echo "Activating virtual environment..."
source "$(pwd)/scripts/airflow3/venv/bin/activate"

# Install dependencies in the virtual environment
echo "Installing dependencies..."
uv pip install -r "$(pwd)/scripts/airflow3/requirements.txt"

# Install Cosmos
uv pip install build
rm -rf "$(pwd)/dist/"
python3 -m build
for wheel in "$(pwd)"/dist/*.whl; do
    uv pip install "$wheel"
done

uv pip install dbt-core
uv pip install dbt-postgres

echo "UV virtual environment setup and dependencies installed successfully!"
