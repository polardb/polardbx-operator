# PolarDB-X Operator 介绍

---

PolarDB-X Operator 是一个基于 Kubernetes 的 PolarDB-X 集群管控系统，希望能在 Kubernetes 上提供完整的生命周期管理能力。PolarDB-X Operator 支持运行在私有或者公有的 Kubernetes 集群上安装并部署 PolarDB-X 集群。

## 限制与说明

### 操作系统和 CPU 架构

PolarDB-X Operator 支持在任意环境的 Kubernetes 集群上进行部署，支持异构 Kubernetes 上的组件部署和 PolarDB-X 数据库集群部署。

目前 PolarDB-X Operator 和 PolarDB-X 集群支持以下操作系统和架构：

| 操作系统 |    CPU 架构     |       推荐配置        |
| :------: | :-------------: | :-------------------: |
|  Linux   | x86_64 (amd64)  | 32C128G, >= 500G 磁盘 |
|  Linux   | aarch64 (arm64) | 32C128G, >= 500G 磁盘 |

注: arm64 架构需要暂无镜像，需要单独编译。

### 磁盘

出于磁盘性能考虑，PolarDB-X Operator 使用宿主机上本地盘的某个路径来存放系统脚本和存储节点的数据，默认配置为 `/data`。PolarDB-X Operator 会自动管理其中存放的脚本和数据，请勿随意删除或更改，以免导致系统和 PolarDB-X 集群出现问题。

若您需要配置不同的路径，可以在安装 Operator 时参考 [[部署文档-Operator部署]](./deploy/install.md) 文档修改配置。

## 安装

在部署 PolarDB-X 集群前，首先需要在 Kubernetes 上安装 PolarDB-X Operator 的系统。通过借助 Kubernetes 上的包管理工具 helm，你可以快速完成系统的部署，参考文档 [[部署文档-快速开始]](./deploy/quick-start.md) 在本地或已有的 Kubernetes 上安装 PolarDB-X Operator 并部署一个 PolarDB-X 测试集群。

Helm 包中预定义了许多配置，如果你想更改这些配置，可以参考 [[部署文档-Operator部署]](./deploy/install.md) 更改配置项，以使它更好的使用 Kubernetes 的资源。

> 注：为了在本地测试，快速开始中的集群使用了较少的资源，如需进行性能测试，请参考运维指南和 PolarDBXCluster API 文档进行更为规范的部署。

## API

为了使 PolarDB-X 能够被 Kubernetes 识别和管理，我们将 PolarDB-X 集群和它的运维操作抽象为多个[定制资源](https://kubernetes.io/zh/docs/concepts/extend-kubernetes/api-extension/custom-resources/):

+ PolarDBXCluster，定义和描述了 PolarDB-X 集群的拓扑、规格、配置和运维等信息
+ XStore，定义和描述了 PolarDB-X 集群的数据节点 (DN) 的拓扑、规格、配置和运维等信息

您可以使用以下命令来查看 Kubernetes 集群中的这些资源:

```bash
kubectl get polardbxcluster,xstore
```

参考 [[API 文档](./api/index.md)] 来了解目前支持的所有资源和细节。

## 运维

同公有云上的 PolarDB-X 集群一样，PolarDB-X Operator 也支持绝大部分的运维操作，包括部署、删除、升级、升配、扩缩容和动态配置等，您可以参考 [[运维指南](./manage/overview.md)] 来了解目前支持的所有的运维操作和使用方法。

## 开发

如果您对 PolarDB-X Operator 的实现感兴趣，您可以阅读 [[开发指南](./develop/index.md)] 来了解整个系统的结构。
