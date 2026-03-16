#!/bin/bash
# 自动化部署脚本 - PolarDB-X Dashboard
# 使用方法: ./tools/dashboard/deploy-all-in-one.sh [--kubeconfig /path/to/kubeconfig]

set -e

# repo root（允许从任意目录执行）
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DOCKERFILE_PATH="${ROOT_DIR}/tools/dashboard/Dockerfile"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 默认配置
NAMESPACE="polardbx-system"
IMAGE_NAME="polardbx-dashboard"
IMAGE_TAG="latest"
KUBECONFIG_PATH="${HOME}/.kube/config"
BUILD_IMAGE=true

# K8s object names (keep consistent with helm-example)
APP_NAME="polardbx-dashboard-backend"
CONFIGMAP_NAME="${APP_NAME}-config"
SECRET_NAME="${APP_NAME}-secret"

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --kubeconfig)
            KUBECONFIG_PATH="$2"
            shift 2
            ;;
        --no-build)
            BUILD_IMAGE=false
            shift
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --image-tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --kubeconfig PATH    Path to kubeconfig file (default: ~/.kube/config)"
            echo "  --no-build          Skip Docker image build"
            echo "  --namespace NAME    Kubernetes namespace (default: polardbx-system)"
            echo "  --image-tag TAG     Docker image tag (default: latest)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo -e "${GREEN}=== PolarDB-X Dashboard 部署脚本 ===${NC}"
echo ""

# 检查前置条件
echo -e "${YELLOW}[1/8] 检查前置条件...${NC}"

command -v docker >/dev/null 2>&1 || { echo -e "${RED}错误: 未找到 docker 命令${NC}"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo -e "${RED}错误: 未找到 kubectl 命令${NC}"; exit 1; }

if [ ! -f "$KUBECONFIG_PATH" ]; then
    echo -e "${RED}错误: kubeconfig 文件不存在: $KUBECONFIG_PATH${NC}"
    exit 1
fi

# 检查 kubeconfig 是否有效
if ! kubectl --kubeconfig="$KUBECONFIG_PATH" cluster-info >/dev/null 2>&1; then
    echo -e "${RED}错误: 无法连接到 Kubernetes 集群，请检查 kubeconfig${NC}"
    exit 1
fi

echo -e "${GREEN}✓ 前置条件检查通过${NC}"
echo ""

# 构建镜像
if [ "$BUILD_IMAGE" = true ]; then
    echo -e "${YELLOW}[2/8] 构建 Docker 镜像...${NC}"
    if docker build -f "${DOCKERFILE_PATH}" -t ${IMAGE_NAME}:${IMAGE_TAG} "${ROOT_DIR}"; then
        echo -e "${GREEN}✓ 镜像构建成功${NC}"
    else
        echo -e "${RED}错误: 镜像构建失败${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}[2/8] 跳过镜像构建（使用 --no-build）${NC}"
fi
echo ""

# 检查镜像是否存在
if ! docker image inspect ${IMAGE_NAME}:${IMAGE_TAG} >/dev/null 2>&1; then
    echo -e "${RED}错误: 镜像不存在: ${IMAGE_NAME}:${IMAGE_TAG}${NC}"
    echo "请先构建镜像或移除 --no-build 参数"
    exit 1
fi

# 创建命名空间
echo -e "${YELLOW}[3/8] 创建命名空间...${NC}"
if kubectl --kubeconfig="$KUBECONFIG_PATH" get namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo -e "${GREEN}✓ 命名空间已存在: $NAMESPACE${NC}"
else
    kubectl --kubeconfig="$KUBECONFIG_PATH" create namespace "$NAMESPACE"
    echo -e "${GREEN}✓ 命名空间创建成功: $NAMESPACE${NC}"
fi
echo ""

# 创建 ConfigMap
echo -e "${YELLOW}[4/8] 创建 ConfigMap...${NC}"
kubectl --kubeconfig="$KUBECONFIG_PATH" create configmap ${CONFIGMAP_NAME} \
    --from-literal=LOG_LEVEL=info \
    --from-literal=LISTEN_ADDRESS=:8080 \
    --from-literal=UI_STATIC_DIR=/app/ui \
    --from-literal=KUBE_MODE=kubeconfig \
    --from-literal=KUBECONFIG_PATH=/etc/kube/kubeconfig \
    -n "$NAMESPACE" \
    --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_PATH" apply -f -
echo -e "${GREEN}✓ ConfigMap 创建/更新成功${NC}"
echo ""

# 创建 Secret
echo -e "${YELLOW}[5/8] 创建 Secret（包含 kubeconfig）...${NC}"
kubectl --kubeconfig="$KUBECONFIG_PATH" create secret generic ${SECRET_NAME} \
    --from-literal=kubeconfig="$(cat "$KUBECONFIG_PATH")" \
    -n "$NAMESPACE" \
    --dry-run=client -o yaml | kubectl --kubeconfig="$KUBECONFIG_PATH" apply -f -
echo -e "${GREEN}✓ Secret 创建/更新成功${NC}"
echo ""

# 创建 Deployment 和 Service
echo -e "${YELLOW}[6/8] 创建 Deployment 和 Service...${NC}"

# 生成 Deployment YAML
cat <<EOF | kubectl --kubeconfig="$KUBECONFIG_PATH" apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${APP_NAME}
  namespace: $NAMESPACE
  labels:
    app: ${APP_NAME}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${APP_NAME}
  template:
    metadata:
      labels:
        app: ${APP_NAME}
    spec:
      containers:
      - name: backend
        image: ${IMAGE_NAME}:${IMAGE_TAG}
        imagePullPolicy: IfNotPresent
        ports:
        - name: http
          containerPort: 8080
        envFrom:
        - configMapRef:
            name: ${CONFIGMAP_NAME}
        - secretRef:
            name: ${SECRET_NAME}
        volumeMounts:
        - name: kubeconfig
          mountPath: /etc/kube
          readOnly: true
        # Note: If kubeconfig references external certificate files (e.g., minikube),
        # you may need to mount additional volumes or use ServiceAccount with in-cluster config
        resources:
          requests:
            cpu: 100m
            memory: 128Mi
          limits:
            cpu: 500m
            memory: 512Mi
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 10
          periodSeconds: 5
      volumes:
      - name: kubeconfig
        secret:
          secretName: ${SECRET_NAME}
          items:
          - key: kubeconfig
            path: kubeconfig
---
apiVersion: v1
kind: Service
metadata:
  name: ${APP_NAME}
  namespace: $NAMESPACE
  labels:
    app: ${APP_NAME}
spec:
  type: ClusterIP
  ports:
  - port: 8080
    targetPort: http
    protocol: TCP
    name: http
  selector:
    app: ${APP_NAME}
EOF

echo -e "${GREEN}✓ Deployment 和 Service 创建成功${NC}"
echo ""

# 等待 Pod 就绪
echo -e "${YELLOW}[7/8] 等待 Pod 就绪...${NC}"
if kubectl --kubeconfig="$KUBECONFIG_PATH" wait --for=condition=ready pod \
    -l app=${APP_NAME} \
    -n "$NAMESPACE" \
    --timeout=120s >/dev/null 2>&1; then
    echo -e "${GREEN}✓ Pod 已就绪${NC}"
else
    echo -e "${YELLOW}⚠ Pod 可能还在启动中，请稍后检查${NC}"
fi
echo ""

# 显示访问信息
echo -e "${YELLOW}[8/8] 部署完成！${NC}"
echo ""
echo -e "${GREEN}=== 访问信息 ===${NC}"
echo ""
echo "命名空间: $NAMESPACE"
echo "Service: ${APP_NAME}"
echo ""
echo "访问方式："
echo ""
echo "1. 端口转发（本地访问）："
echo "   kubectl --kubeconfig=\"$KUBECONFIG_PATH\" port-forward -n $NAMESPACE svc/${APP_NAME} 8080:8080"
echo "   然后访问: http://localhost:8080"
echo ""
echo "2. 查看 Pod 状态："
echo "   kubectl --kubeconfig=\"$KUBECONFIG_PATH\" get pods -n $NAMESPACE"
echo ""
echo "3. 查看日志："
echo "   kubectl --kubeconfig=\"$KUBECONFIG_PATH\" logs -n $NAMESPACE -l app=${APP_NAME}"
echo ""
echo "4. 查看 Service："
echo "   kubectl --kubeconfig=\"$KUBECONFIG_PATH\" get svc -n $NAMESPACE"
echo ""

