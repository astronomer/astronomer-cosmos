#!/bin/bash

# Exit on error
set -e

# Create a UV virtual environment named 'env' (you can change this as needed)
echo "Creating UV virtual environment at $(pwd)/tools"
uv venv airflow3-env --directory "$(pwd)/tools"

# Activate the virtual environment
echo "Activating virtual environment..."
source "$(pwd)/tools/airflow3-env/bin/activate"

# Install dependencies in the virtual environment
echo "Installing dependencies..."
uv pip install -r tools/requirements.txt

# Optional: Install additional dependencies
uv pip install astronomer-cosmos

echo "UV virtual environment setup and dependencies installed successfully!"
