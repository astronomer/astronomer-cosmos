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
pip install -U "dbt-core>=$DBT_VERSION,<$NEXT_MINOR_VERSION" dbt-postgres

if [ -z "${AIRFLOW_CONN_AWS_S3_CONN:-}" ]; then
  echo "AIRFLOW_CONN_AWS_S3_CONN must be set for kubernetes docs upload tests."
  exit 1
fi

read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION <<EOF
$(python - <<'PY'
import os
from urllib.parse import parse_qs, unquote, urlparse

uri = os.environ["AIRFLOW_CONN_AWS_S3_CONN"]
parsed = urlparse(uri)
query = parse_qs(parsed.query)

access_key = unquote(parsed.username or "")
secret_key = unquote(parsed.password or "")
region = query.get("region_name", [None])[0] or query.get("region", [None])[0] or "us-east-1"

if not access_key or not secret_key:
    raise SystemExit("AIRFLOW_CONN_AWS_S3_CONN must include AWS access key and secret key.")
PY
)
EOF

# Create a Kubernetes secret named 'postgres-secrets' with the specified literals for host and password
set +x
kubectl create secret generic postgres-secrets \
  --from-literal=host=postgres-postgresql.default.svc.cluster.local \
  --from-literal=password=postgres

kubectl create secret generic aws-s3-secrets \
  --from-literal=aws_access_key_id="$AWS_ACCESS_KEY_ID" \
  --from-literal=aws_secret_access_key="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=aws_default_region="$AWS_DEFAULT_REGION"
set -x

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
