#!/usr/bin/env bash
# One-shot GCP resource provisioning and Kubernetes deployment setup
set -euo pipefail

PROJECT_ID=${1:-$(gcloud config get-value project)}
REGION=${2:-"us-central1"}
CLUSTER_NAME="telemetry-cluster"

echo "=== Provisioning Fleet Telemetry Pipeline on GCP Project: $PROJECT_ID ($REGION) ==="

echo "1. Enabling required Google Cloud APIs..."
gcloud services enable \
  container.googleapis.com \
  bigtable.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  monitoring.googleapis.com \
  cloudtrace.googleapis.com \
  --project="$PROJECT_ID"

echo "2. Initializing and applying Terraform resources..."
cd terraform
terraform init
terraform apply -auto-approve -var="project_id=$PROJECT_ID" -var="region=$REGION"
cd ..

echo "3. Retrieving GKE cluster credentials..."
gcloud container clusters get-credentials "$CLUSTER_NAME" --region="$REGION" --project="$PROJECT_ID"

echo "4. Creating Kubernetes secrets for GCP access..."
kubectl create namespace fleet --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic gcp-config \
  --namespace fleet \
  --from-literal=project_id="$PROJECT_ID" \
  --from-literal=bigtable_instance="fleet-telemetry-bt" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "=== GCP environment setup complete! ==="
