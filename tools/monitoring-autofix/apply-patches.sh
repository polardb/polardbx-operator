#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="polardbx-monitor"
SERVICE_ACCOUNT=""

usage() {
  cat <<'EOF'
Usage: apply-patches.sh [--namespace <ns>] --service-account <name>

Adds monitoring auto-fix annotations to the deployed resources and
creates a scoped RBAC bundle for the wizard/installer service account.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --service-account)
      SERVICE_ACCOUNT="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$SERVICE_ACCOUNT" ]]; then
  echo "--service-account is required" >&2
  usage
  exit 1
fi

echo "Annotating monitoring resources in namespace ${NAMESPACE}..."
kubectl annotate --overwrite prometheus.monitoring.coreos.com/k8s \
  -n "$NAMESPACE" monitoring.polardbx.com/auto-fix-ids="monitoring::prometheus::restart"

kubectl annotate --overwrite deployment/grafana \
  -n "$NAMESPACE" monitoring.polardbx.com/auto-fix-ids="monitoring::grafana::restore-admin-secret"

kubectl annotate --overwrite alertmanager.monitoring.coreos.com/main \
  -n "$NAMESPACE" monitoring.polardbx.com/auto-fix-ids="monitoring::alertmanager::refresh-config"

echo "Applying RBAC overlay for service account ${SERVICE_ACCOUNT}..."
cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: polardbx-monitor-autofix
rules:
- apiGroups: ["apps"]
  resources: ["statefulsets", "deployments", "daemonsets"]
  verbs: ["get", "list", "watch", "patch"]
- apiGroups: [""]
  resources: ["services", "pods"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["create", "delete", "get", "list", "watch"]
- apiGroups: ["monitoring.coreos.com"]
  resources: ["prometheuses", "alertmanagers"]
  verbs: ["get", "list", "watch", "patch"]
- apiGroups: ["polardbx.aliyun.com"]
  resources: ["polardbxmonitors"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: polardbx-monitor-autofix
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: polardbx-monitor-autofix
subjects:
- kind: ServiceAccount
  name: ${SERVICE_ACCOUNT}
  namespace: ${NAMESPACE}
EOF

echo "Auto-fix overlay applied successfully."
