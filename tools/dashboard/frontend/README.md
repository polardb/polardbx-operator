# PolarDB-X UI

一个用于管理 PolarDB-X 集群的现代化 Web 界面，基于 Angular 19 和 Go 构建。

## 项目概述

PolarDB-X UI 提供了一个直观的 Web 界面来管理 Kubernetes 上的 PolarDB-X 集群。该项目包含前端 Angular 应用和后端 Go API 服务，支持集群的创建、监控、备份和参数管理等功能。

## 技术栈

### 前端
- **Angular 19** - 现代化前端框架
- **Angular Material** - UI 组件库
- **TypeScript** - 类型安全的 JavaScript
- **RxJS** - 响应式编程
- **SCSS** - CSS 预处理器

### 后端
- **Go** - 高性能后端语言
- **Gin** - Web 框架
- **Kubernetes Client-Go** - Kubernetes API 客户端
- **Controller-Runtime** - Kubernetes 控制器运行时

## 功能特性

### ✅ 已实现功能
- **集群连接管理**
  - kubeconfig 验证和连接
  - 多集群支持
  - 连接状态监控

- **集群管理**
  - 集群列表查看
  - 集群状态监控
  - 集群详情查看
  - 集群删除操作

- **实时数据**
  - 自动刷新机制（每5秒）
  - 实时状态更新
  - 响应式设计

### 🔄 开发中功能
- 集群创建和编辑
- 备份管理
- 参数模板管理
- 监控和告警
- 日志查看

## 快速开始

### 前置要求

- Node.js 18+ 
- Go 1.19+
- Kubernetes 集群（支持 PolarDB-X Operator）
- 有效的 kubeconfig 文件

### 安装和运行

#### 1. 克隆项目
```bash
cd polardbx-operator/polardbx-ui
```

#### 2. 安装前端依赖
```bash
npm install
```

#### 3. 启动后端服务
```bash
cd ../backend
go run main.go
```

后端服务将在 `http://localhost:8080` 启动

#### 4. 启动前端应用
```bash
cd ../polardbx-ui
ng serve
```

前端应用将在 `http://localhost:4200` 启动

#### 5. 运行端到端测试（可选）
首次执行前安装 Playwright 浏览器依赖：
```bash
npx playwright install --with-deps
```

随后可在前端目录运行自动化场景：
```bash
npm run e2e
```

测试完成后将在 `playwright-report/` 生成 HTML 报告。
## 开发代理配置（解决 CORS，推荐）

开发环境下建议通过 Angular 代理将 `/api/*` 请求转发到后端，以避免浏览器跨域（CORS）问题。

1) 在 `polardbx-ui` 根目录创建 `proxy.conf.json`：

```json
{
  "/api/*": {
    "target": "http://localhost:8080",
    "secure": false,
    "changeOrigin": true,
    "logLevel": "debug"
  }
}
```

2) 启动开发服务器时启用代理：

```bash
ng serve --proxy-config proxy.conf.json
```

3) 访问地址保持不变：`http://localhost:4200`


#### 5. 访问应用

打开浏览器访问 `http://localhost:4200`，您将看到连接页面。

## 使用指南

### 连接到 Kubernetes 集群

1. 在连接页面，粘贴您的 kubeconfig 文件内容
2. 点击"连接"按钮
3. 连接成功后将自动跳转到集群列表页面

### kubeconfig 获取方式

#### 本地集群
```bash
cat ~/.kube/config
```

#### 云服务提供商
- **阿里云 ACK**: 在控制台下载集群的 kubeconfig
- **腾讯云 TKE**: 在集群管理页面获取访问凭证
- **AWS EKS**: 使用 `aws eks update-kubeconfig` 命令

### 集群管理

- **查看集群列表**: 连接成功后自动显示
- **查看集群详情**: 点击集群名称
- **删除集群**: 点击操作列的删除按钮
- **刷新数据**: 点击刷新按钮或等待自动刷新
- **断开连接**: 点击断开连接按钮返回连接页面

## API 文档

### 认证

除了 `/api/v1/connect` 端点外，所有 API 都需要在请求头中包含 base64 编码的 kubeconfig：

```
X-Kubeconfig-B64: <base64-encoded-kubeconfig>
```

### 主要端点

#### 连接验证
```http
POST /api/v1/connect
Content-Type: text/plain

<kubeconfig-content>
```

#### 集群管理
```http
# 获取集群列表
GET /api/v1/clusters

# 获取集群详情
GET /api/v1/clusters/{name}

# 创建集群
POST /api/v1/clusters

# 更新集群
PUT /api/v1/clusters/{name}

# 删除集群
DELETE /api/v1/clusters/{name}
```

#### 备份管理
```http
# 获取备份列表
GET /api/v1/clusters/{name}/backups

# 创建备份
POST /api/v1/clusters/{name}/backups
```

#### 参数管理
```http
# 获取参数列表
GET /api/v1/parameters

# 创建参数模板
POST /api/v1/parameters

# 获取参数详情
GET /api/v1/parameters/{name}

# 更新参数
PUT /api/v1/parameters/{name}

# 删除参数
DELETE /api/v1/parameters/{name}
```

## 开发指南

### 项目结构

```
polardbx-ui/
├── src/
│   ├── app/
│   │   ├── pages/
│   │   │   ├── connect/          # 连接页面
│   │   │   └── cluster-list/     # 集群列表页面
│   │   ├── services/
│   │   │   └── api.service.ts    # API 服务
│   │   ├── app.component.*       # 根组件
│   │   ├── app.config.ts         # 应用配置
│   │   └── app.routes.ts         # 路由配置
│   ├── styles.scss               # 全局样式
│   └── main.ts                   # 应用入口
├── backend/
│   ├── main.go                   # 后端入口
│   └── pkg/
│       ├── api/                  # API 处理器
│       └── k8s/                  # Kubernetes 客户端
└── README.md
```

### 开发环境配置

#### 前端开发
```bash
# 安装依赖
npm install

# 启动开发服务器
ng serve

# 构建生产版本
ng build

# 运行测试
ng test

# 运行 Playwright 端到端测试
npm run e2e

# 运行带界面的 E2E 调试
npm run e2e:headed
```

#### 后端开发
```bash
# 安装依赖
go mod tidy

# 启动开发服务器
go run main.go

# 构建二进制文件
go build -o polardbx-ui-backend

# 运行测试
go test ./...
```

### 代码规范

- **前端**: 遵循 Angular 官方风格指南
- **后端**: 遵循 Go 官方代码规范
- **提交**: 使用语义化提交信息

## 部署

### Docker 部署

#### 构建镜像
```bash
# 构建前端镜像
docker build -t polardbx-ui-frontend .

# 构建后端镜像
cd backend
docker build -t polardbx-ui-backend .
```

#### 运行容器
```bash
# 运行后端
docker run -p 8080:8080 polardbx-ui-backend

# 运行前端
docker run -p 4200:4200 polardbx-ui-frontend
```

### Kubernetes 部署

```yaml
# 示例部署配置
apiVersion: apps/v1
kind: Deployment
metadata:
  name: polardbx-ui
spec:
  replicas: 1
  selector:
    matchLabels:
      app: polardbx-ui
  template:
    metadata:
      labels:
        app: polardbx-ui
    spec:
      containers:
      - name: backend
        image: polardbx-ui-backend:latest
        ports:
        - containerPort: 8080
      - name: frontend
        image: polardbx-ui-frontend:latest
        ports:
        - containerPort: 4200
```

## 故障排除

### 常见问题

#### 1. 连接失败
- 检查 kubeconfig 文件格式是否正确
- 确认 Kubernetes 集群可访问
- 验证用户权限是否足够

#### 2. API 调用失败
- 检查后端服务是否正常运行
- 确认 CORS 配置是否正确
- 验证请求头中的 kubeconfig 是否有效

#### 3. 前端构建错误
- 清除 node_modules 并重新安装
- 检查 Node.js 版本是否兼容
- 确认所有依赖都已正确安装

### 日志查看

#### 前端日志
- 浏览器开发者工具控制台
- Angular 开发服务器输出

#### 后端日志
- 标准输出（开发环境）
- 日志文件（生产环境）

## 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 许可证

本项目采用 Apache 2.0 许可证。详情请参阅 [LICENSE](../LICENSE) 文件。

## 支持

如果您遇到问题或有建议，请：

1. 查看 [故障排除](#故障排除) 部分
2. 搜索现有的 [Issues](../../issues)
3. 创建新的 Issue 描述问题
4. 参考 [测试报告](./TEST_REPORT.md) 了解已知问题

## 更新日志

### v1.0.0 (2025-07-19)
- ✅ 初始版本发布
- ✅ 实现基本的集群连接和管理功能
- ✅ 完成前后端基础架构
- ✅ 添加 Angular Material UI 组件
- ✅ 实现 kubeconfig 认证机制
- ✅ 支持集群列表查看和基本操作

---

**注意**: 本项目仍在积极开发中，功能和 API 可能会发生变化。建议在生产环境使用前进行充分测试。
