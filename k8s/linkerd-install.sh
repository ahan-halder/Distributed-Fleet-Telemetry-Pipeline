#!/usr/bin/env bash
set -euo pipefail

echo "Installing Linkerd CLI..."
curl -sL https://run.linkerd.io/install | sh
export PATH=$PATH:$HOME/.linkerd2/bin

linkerd version

echo "Validating Kubernetes cluster..."
linkerd check --pre

echo "Installing Linkerd CRDs..."
linkerd install --crds | kubectl apply -f -

echo "Installing Linkerd control plane..."
linkerd install | kubectl apply -f -

linkerd check

echo "Installing Linkerd viz extension..."
linkerd viz install | kubectl apply -f -
linkerd check

echo "Annotating fleet namespace for automatic proxy injection..."
kubectl create namespace fleet --dry-run=client -o yaml | kubectl apply -f -
kubectl annotate namespace fleet linkerd.io/inject=enabled --overwrite

echo "Linkerd service mesh installation and configuration complete!"
