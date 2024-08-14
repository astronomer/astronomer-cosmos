#!/bin/bash

set -x
set -e

#check_nodes_ready() {
#    # Get the list of node statuses
#    node_statuses=$(kubectl get nodes --no-headers | awk '{print $2}')
#    # Check if all nodes are in the "Ready" state
#    for status in $node_statuses; do
#        if [ "$status" != "Ready" ]; then
#            return 1
#        fi
#    done
#    return 0
#}
#
#wait_for_nodes_ready() {
#    local max_attempts=60
#    local interval=5
#    local attempt=0
#
#    echo "Waiting for nodes in the kind cluster to be in 'Ready' state..."
#
#    while [ $attempt -lt $max_attempts ]; do
#        if check_nodes_ready; then
#            echo "All nodes in the kind cluster are in 'Ready' state."
#            return 0
#        else
#            echo "Nodes are not yet ready. Checking again in $interval seconds..."
#            sleep $interval
#            attempt=$((attempt + 1))
#        fi
#    done
#
#    echo "Timeout waiting for nodes in the kind cluster to be in 'Ready' state."
#    return 1
#}
#
#kubectl config set-context default
#
## Create a docker image containing the dbt project files and dbt profile
#cd dev && docker build -t dbt-jaffle-shop:1.0.0 -f Dockerfile.postgres_profile_docker_k8s .
## Make the build image available in the Kind K8s cluster
#kind load docker-image dbt-jaffle-shop:1.0.0
#
## Deploy a Postgres pod to Kind
##helm repo add bitnami https://charts.bitnami.com/bitnami
##helm repo update
##helm install postgres bitnami/postgresql --set postgresqlExtendedConf.huge_pages="off" # -f scripts/test/values.yaml
#
## Retrieve the Postgres password and set it as an environment variable
##POSTGRES_PASSWORD=$(kubectl get secret --namespace default postgres-postgresql -o jsonpath="{.data.postgres-password}" | base64 -d)
##export POSTGRES_PASSWORD
#
#kubectl create secret generic postgres-secrets --from-literal=host=postgres-postgresql.default.svc.cluster.local --from-literal=password=$POSTGRES_PASSWORD
#
#sleep 120
## Expose the Postgres to the host running Docker/Kind
##kubectl port-forward --namespace default postgres-postgresql-0  5432:5432 &
##kubectl port-forward --namespace default svc/postgres-postgresql 5432:5432 &
##wait_for_nodes_ready
##
### Wait for the kind cluster to be in 'Ready' state
##wait_for_nodes_ready
#
## For Debugging
#echo "nodes"
#kubectl get nodes
#echo "helm"
#helm list
#echo "pod service"
#kubectl get pods --namespace default
#kubectl get svc --namespace default
#echo "pg log"
#kubectl logs postgres-postgresql-0 -c postgresql
#kubectl describe pod postgres-postgresql-0


kubectl create secret generic postgres-secrets --from-literal=host=postgres-postgresql.default.svc.cluster.local --from-literal=password=postgres

kubectl apply -f scripts/test/postgres-deployment.yaml

cd dev && docker build -t dbt-jaffle-shop:1.0.0 -f Dockerfile.postgres_profile_docker_k8s .
kind load docker-image dbt-jaffle-shop:1.0.0

POD_NAME=$(kubectl get pods -n default -l app=postgres -o jsonpath='{.items[0].metadata.name}')

echo "$POD_NAME"

kubectl port-forward --namespace default "$POD_NAME"  5432:5432 &

kubectl get pod
