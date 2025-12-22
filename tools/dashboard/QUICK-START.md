# 快速开始指南 - PolarDB-X Dashboard 部署

本指南帮助新用户快速部署 PolarDB-X Dashboard 到 Kubernetes 集群。

## 前置条件

- 已下载本仓库
- 已启动 Kubernetes 集群（本地或远程）
- 有 kubeconfig 文件（通常位于 `~/.kube/config`）
- 已安装 Docker
- 已安装 kubectl
- 已安装 Helm（可选，用于 Helm 部署）

## 方式一：快速部署脚本（推荐）

### 1. 构建镜像

```bash
# 在项目根目录执行
docker build -f tools/dashboard/Dockerfile -t polardbx-dashboard:latest .
```

### 1.1（仅本地集群）加载镜像到节点

如果你用的是 `minikube/kind` 这种本地集群，需要把镜像加载进集群节点，否则 Pod 会拉取不到 `polardbx-dashboard:latest`：

```bash
# minikube
minikube image load polardbx-dashboard:latest

# kind
kind load docker-image polardbx-dashboard:latest
```

### 2. 运行自动化部署脚本

```bash
# 使用默认 kubeconfig (~/.kube/config)
./tools/dashboard/deploy-all-in-one.sh

# 或指定 kubeconfig 路径
./tools/dashboard/deploy-all-in-one.sh --kubeconfig /path/to/kubeconfig
```

脚本会自动：
- 检查环境
- 创建命名空间
- 构建镜像（如果未构建）
- 创建 ConfigMap 和 Secret
- 部署 Deployment 和 Service
- 等待 Pod 就绪
- 显示访问方式

## 方式二：手动部署（分步执行）

### 步骤 1：构建镜像

```bash
docker build -f tools/dashboard/Dockerfile -t polardbx-dashboard:latest .
```

### 步骤 2：创建命名空间

```bash
kubectl create namespace polardbx-system
```

### 步骤 3：创建 ConfigMap

```bash
kubectl create configmap polardbx-dashboard-backend-config \
  --from-literal=LOG_LEVEL=info \
  --from-literal=LISTEN_ADDRESS=:8080 \
  --from-literal=UI_STATIC_DIR=/app/ui \
  --from-literal=KUBE_MODE=kubeconfig \
  --from-literal=KUBECONFIG_PATH=/etc/kube/kubeconfig \
  -n polardbx-system
```

### 步骤 4：创建 Secret（包含 kubeconfig）

```bash
kubectl create secret generic polardbx-dashboard-backend-secret \
  --from-literal=kubeconfig="$(cat ~/.kube/config)" \
  -n polardbx-system
```

### 步骤 5：部署应用

```bash
# 使用 kubectl apply
kubectl apply -f tools/k8s-manifests/ -n polardbx-system

# 或使用 Helm
helm install polardbx-dashboard tools/dashboard/helm-example \
  --set backend.image.repository=polardbx-dashboard \
  --set backend.image.tag=latest \
  --set backend.config.kubeMode=kubeconfig \
  --set-file backend.secrets.kubeconfig=~/.kube/config \
  --namespace polardbx-system
```

### 步骤 6：检查状态

```bash
# 查看 Pod 状态
kubectl get pods -n polardbx-system

# 查看日志
kubectl logs -n polardbx-system deploy/polardbx-dashboard-backend

# 查看 Service
kubectl get svc -n polardbx-system
```

### 步骤 7：访问应用

```bash
# 方式 1：端口转发（本地访问）
kubectl port-forward -n polardbx-system svc/polardbx-dashboard-backend 8080:8080
# 然后访问 http://localhost:8080

# 方式 2：NodePort（如果 Service 类型是 NodePort）
kubectl get svc -n polardbx-system polardbx-dashboard-backend
# 使用 <NodeIP>:<NodePort> 访问

# 方式 3：LoadBalancer（云环境）
kubectl get svc -n polardbx-system polardbx-dashboard-backend
# 使用 EXTERNAL-IP 访问
```

## 方式三：使用 Helm Chart（推荐生产环境）

### 1. 准备 values 文件

创建 `my-values.yaml`：

```yaml
backend:
  image:
    repository: polardbx-dashboard
    tag: latest
  config:
    logLevel: info
    kubeMode: kubeconfig
  secrets:
    kubeconfig: ""  # 将在部署时通过命令行设置
```

### 2. 部署

```bash
# 部署
helm install polardbx-dashboard tools/dashboard/helm-example \
  --namespace polardbx-system \
  --create-namespace \
  -f my-values.yaml \
  --set-file backend.secrets.kubeconfig=~/.kube/config
```

### 3. 升级

```bash
helm upgrade polardbx-dashboard tools/dashboard/helm-example \
  --namespace polardbx-system \
  -f my-values.yaml \
  --set-file backend.secrets.kubeconfig=~/.kube/config
```

## 验证部署

### 1. 健康检查

```bash
# 通过端口转发测试
kubectl port-forward -n polardbx-system svc/polardbx-dashboard-backend 8080:8080 &

# 测试健康端点
curl http://localhost:8080/api/v1/health

# 测试前端页面
curl http://localhost:8080/ | head -20
```

### 2. 功能测试

1. 打开浏览器访问 `http://localhost:8080`
2. 在 UI 中连接 Kubernetes 集群（使用 kubeconfig）
3. 查看节点列表
4. 测试终端功能

## 故障排查

### Pod 无法启动

```bash
# 查看 Pod 状态
kubectl describe deploy -n polardbx-system polardbx-dashboard-backend

# 查看日志
kubectl logs -n polardbx-system deploy/polardbx-dashboard-backend
```

### 无法连接 K8s API

```bash
# 检查 kubeconfig 是否正确注入
kubectl exec -n polardbx-system <pod-name> -- cat /etc/kube/kubeconfig

# 检查环境变量
kubectl exec -n polardbx-system <pod-name> -- env | grep KUBE
```

### 前端页面无法加载

```bash
# 检查静态文件目录
kubectl exec -n polardbx-system <pod-name> -- ls -la /app/ui/

# 检查 UI_STATIC_DIR 环境变量
kubectl exec -n polardbx-system <pod-name> -- env | grep UI_STATIC_DIR
```

## 卸载

```bash
# 使用 Helm
helm uninstall polardbx-dashboard -n polardbx-system

# 或使用 kubectl
kubectl delete -f tools/k8s-manifests/ -n polardbx-system
kubectl delete namespace polardbx-system
```
