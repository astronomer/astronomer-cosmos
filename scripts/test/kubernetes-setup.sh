#!/bin/bash

# Print each command before executing it
# Exit the script immediately if any command exits with a non-zero status (for debugging purposes)
set -x
set -e

DBT_VERSION="$1"
echo "DBT_VERSION:"
echo "$DBT_VERSION"
NEXT_MINOR_VERSION=$(echo "$DBT_VERSION" | awk -F. '{print $1"."$2+1}')

pip uninstall dbt-adapters dbt-common dbt-core dbt-extractor dbt-postgres dbt-semantic-interfaces -y
pip install -U \
  "dbt-core==$DBT_VERSION" \
  "dbt-postgres>=$DBT_VERSION,<$NEXT_MINOR_VERSION"

# apache-airflow-core 3.0.0 requires pydantic>=2.11.0, but the above dbt adapters in case of version 1.6 and 1.9 install
# pydantic 1.10.22 which make it incompatible.
# With pydantic 1.10.22 we get the below error
# File "<...>/hatch/env/virtual/astronomer-cosmos/D9FI7Men/tests.py3.11-3.0-1.6/lib/python3.11/site-packages/cadwyn/_utils.py", line 5, in <module>
#     from pydantic._internal._decorators import unwrap_wrapped_function
# ModuleNotFoundError: No module named 'pydantic._internal'
# Hence, we re-install pydantic with the required minimum version after installing dbt adapters.
# dbt-core 1.9 raises
if [ "$DBT_VERSION" = "1.6" ] || [ "$DBT_VERSION" = "1.9" ]; then
    echo "DBT_VERSION is $DBT_VERSION, installing pydantic==2.11.0 for apache-airflow-core compatibility."
    pip install "pydantic==2.11.0"
fi

# Create a Kubernetes secret named 'postgres-secrets' with the specified literals for host and password
kubectl create secret generic postgres-secrets \
  --from-literal=host=postgres-postgresql.default.svc.cluster.local \
  --from-literal=password=postgres

# Apply the PostgreSQL deployment configuration from the specified YAML file
kubectl apply -f scripts/test/postgres-deployment.yaml

# Build the Docker image with tag 'dbt-jaffle-shop:1.0.0' using the specified Dockerfile
cd dev && docker build --progress=plain --no-cache -t dbt-jaffle-shop:1.0.0 -f Dockerfile.postgres_profile_docker_k8s .

# Load the Docker image into the local KIND cluster
kind load docker-image dbt-jaffle-shop:1.0.0

# Retrieve the name of the PostgreSQL pod using the label selector 'app=postgres'
# The output is filtered to get the first pod's name
POD_NAME=$(kubectl get pods -n default -l app=postgres -o jsonpath='{.items[0].metadata.name}')

# Wait for the PostgreSQL pod to be in the 'Running' and 'Ready' state
echo "Waiting for PostgreSQL pod to be ready..."
while true; do
  POD_STATUS=$(kubectl get pod "$POD_NAME" -n default -o jsonpath='{.status.phase}')

  if [ "$POD_STATUS" = "Running" ] && [ "$(kubectl get pod "$POD_NAME" -n default -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}')" = "True" ]; then
    echo "PostgreSQL pod is up and running!"
    break
  elif [ "$POD_STATUS" = "Error" ]; then
    echo "Error: PostgreSQL pod failed to start. Exiting..."
    kubectl describe pod "$POD_NAME" -n default  # Show details for debugging
    exit 1
  else
    echo "Pod $POD_NAME is not ready yet (status: $POD_STATUS). Waiting..."
    sleep 5
  fi
done
# Print the name of the PostgreSQL pod
echo "$POD_NAME"

# Forward port 5432 from the PostgreSQL pod to the local machine's port 5432
# This allows local access to the PostgreSQL instance running in the pod
kubectl port-forward --namespace default "$POD_NAME" 5432:5432 &

# List all pods in the default namespace to verify the status of pods
kubectl get pod
