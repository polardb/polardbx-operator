# 快速上手

本文介绍了如何创建一个简单的 Kubernetes 集群，部署 PolarDB-X Operator，并使用 operator 部署一个完整的 PolarDB-X 集群。

> 注：本文中的部署说明仅用于测试目的，不要直接用于生产环境。

本文主要包含以下内容：

1. [创建 Kubernetes 测试集群](#创建-kubernetes-测试集群)
2. [部署 PolarDB-X Operator](#部署-polardb-x-operator)
3. [部署 PolarDB-X 集群](#部署-polardb-x-集群)
4. [连接 PolarDB-X 集群](#连接-polardb-x-集群)
5. [销毁 PolarDB-X 集群](#销毁-polardb-x-集群)
6. [卸载 PolarDB-X Operator](#卸载-polardb-x-operator)

# 创建 Kubernetes 测试集群

本节主要介绍如何使用 [minikube](https://minikube.sigs.k8s.io/docs/start/) 创建 Kubernetes 测试集群，您也可以使用阿里云的 [容器服务 ACK](https://www.aliyun.com/product/kubernetes) 来创建一个 Kubernetes 集群，并遵循教程部署 PolarDB-X Operator 和 PolarDB-X 集群。

## 使用 minikube 创建 Kubernetes 集群

[minikube](https://minikube.sigs.k8s.io/docs/start/) 是由社区维护的用于快速创建 Kubernetes 测试集群的工具，适合测试和学习 Kubernetes。使用 minikube 创建的 Kubernetes 集群可以运行在容器或是虚拟机中，本节中以 CentOS 8.2 上创建 Kubernetes 为例。

> 注：如在其他操作系统例如 macOS 或 Windows 上部署 minikube，部分步骤可能略有不同。

部署前，请确保已经安装 minikube 和 Docker，并符合以下要求：

+ 机器规格不小于 4c8g
+ minikube >= 1.18.0
+ docker >= 1.19.3

minikube 要求使用非 root 账号进行部署，如果你试用 root 账号访问机器，需要新建一个账号。

```bash
$ useradd -ms /bin/bash galaxykube
$ usermod -aG docker galaxykube
```

如果你使用其他账号，请和上面一样将它加入 docker 组中，以确保它能够直接访问 docker。

使用 su 切换到账号 `galaxykube`，

```bash
$ su galaxykube
```

执行下面的命令启动一个 minikube，

```bash
minikube start --cpus 4 --memory 7960 --image-mirror-country cn --registry-mirror=https://docker.mirrors.sjtug.sjtu.edu.cn
```

> 注：这里我们使用了阿里云的 minikube 镜像源以及 USTC 提供的 docker 镜像源来加速镜像的拉取。

如果一切运行正常，你将会看到类似下面的输出。

```bash
😄  minikube v1.23.2 on Centos 8.2.2004 (amd64)
✨  Using the docker driver based on existing profile
❗  Your cgroup does not allow setting memory.
    ▪ More information: https://docs.docker.com/engine/install/linux-postinstall/#your-kernel-does-not-support-cgroup-swap-limit-capabilities
❗  Your cgroup does not allow setting memory.
    ▪ More information: https://docs.docker.com/engine/install/linux-postinstall/#your-kernel-does-not-support-cgroup-swap-limit-capabilities
👍  Starting control plane node minikube in cluster minikube
🚜  Pulling base image ...
🤷  docker "minikube" container is missing, will recreate.
🔥  Creating docker container (CPUs=4, Memory=7960MB) ...
    > kubeadm.sha256: 64 B / 64 B [--------------------------] 100.00% ? p/s 0s
    > kubelet.sha256: 64 B / 64 B [--------------------------] 100.00% ? p/s 0s
    > kubectl.sha256: 64 B / 64 B [--------------------------] 100.00% ? p/s 0s
    > kubeadm: 43.71 MiB / 43.71 MiB [---------------] 100.00% 1.01 MiB p/s 44s
    > kubectl: 44.73 MiB / 44.73 MiB [-------------] 100.00% 910.41 KiB p/s 51s
    > kubelet: 146.25 MiB / 146.25 MiB [-------------] 100.00% 2.71 MiB p/s 54s

    ▪ Generating certificates and keys ...
    ▪ Booting up control plane ...
    ▪ Configuring RBAC rules ...
🔎  Verifying Kubernetes components...
    ▪ Using image registry.cn-hangzhou.aliyuncs.com/google_containers/storage-provisioner:v5 (global image repository)
🌟  Enabled addons: storage-provisioner, default-storageclass
💡  kubectl not found. If you need it, try: 'minikube kubectl -- get pods -A'
🏄  Done! kubectl is now configured to use "minikube" cluster and "default" namespace by default
```

此时 minikube 已经正常运行。minikube 将自动设置 kubectl 的配置文件，如果之前已经安装过 kubectl，现在可以使用 kubectl 来访问集群：

```bash
$ kubectl cluster-info
kubectl cluster-info
Kubernetes control plane is running at https://192.168.49.2:8443
CoreDNS is running at https://192.168.49.2:8443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.
```

如果没有安装 kubectl 的，minikube 也提供了子命令来使用 kubectl：

```bash
$ minikube kubectl -- cluster-info
Kubernetes control plane is running at https://192.168.49.2:8443
CoreDNS is running at https://192.168.49.2:8443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.
```

> 注意：minikube kubectl 子命令需要在 kubectl 的参数前加 "--"，如使用 bash shell 可以用 alias kubectl="minikube kubectl -- " 来设置快捷指令。下文都将使用 kubectl 命令进行操作。

现在我们可以开始部署 PolarDB-X Operator 了！

> 测试完成后，执行 minikube delete 来销毁集群。

# 部署 PolarDB-X Operator

开始之前，请确保满足以下前置要求：

+ 已经准备了一个运行中的 Kubernetes 集群，并确保
  + 集群版本 >= 1.18.0
  + 至少有 2 个可分配的 CPU
  + 至少有 4GB 的可分配内存
  + 至少有 30GB 以上的磁盘空间
+ 已经安装了 kubectl 可以访问 Kubernetes 集群
+ 已经安装了 [Helm 3](https://helm.sh/docs/intro/install/)


首先创建一个叫 `polardbx-operator-system` 的命名空间，

```bash
$ kubectl create namespace polardbx-operator-system
```

执行以下命令安装 PolarDB-X Operator。

```bash
$ helm install --namespace polardbx-operator-system polardbx-operator https://github.com/ApsaraDB/galaxykube/releases/download/v1.2.1/polardbx-operator-1.2.0.tgz
```

期望看到如下输出：

```bash
NAME: polardbx-operator
LAST DEPLOYED: Sun Oct 17 15:17:29 2021
NAMESPACE: polardbx-operator-system
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
polardbx-operator is installed. Please check the status of components:

    kubectl get pods --namespace polardbx-operator-system

Now have fun with your first PolarDB-X cluster.

Here's the manifest for quick start:

```yaml
apiVersion: polardbx.aliyun.com/v1
kind: PolarDBXCluster
metadata:
  name: quick-start
  annotations:
    polardbx/topology-mode-guide: quick-start
```

查看 PolarDB-X Operator 组件的运行情况，等待它们都进入 Running 状态：

```bash
$ kubectl get pods --namespace polardbx-operator-system
NAME                                           READY   STATUS    RESTARTS   AGE
polardbx-controller-manager-6c858fc5b9-zrhx9   1/1     Running   0          66s
polardbx-hpfs-d44zd                            1/1     Running   0          66s
polardbx-tools-updater-459lc                   1/1     Running   0          66s
```

恭喜！PolarDB-X Operator 已经安装完成，现在可以开始部署 PolarDB-X 集群了！

# 部署 PolarDB-X 集群

现在我们来快速部署一个 PolarDB-X 集群，它包含 1 个 GMS 节点、1 个 CN 节点、1 个 DN 节点和 1 个 CDC 节点。执行以下命令创建一个这样的集群：

```bash
echo "apiVersion: polardbx.aliyun.com/v1
kind: PolarDBXCluster
metadata:
  name: quick-start
  annotations:
    polardbx/topology-mode-guide: quick-start" | kubectl apply -f -
```

你将看到以下输出：

```bash
polardbxcluster.polardbx.aliyun.com/quick-start created
```

使用如下命令查看创建状态：

```bash
$ kubectl get polardbxcluster -w
NAME          GMS   CN    DN    CDC   PHASE      DISK   AGE
quick-start   0/1   0/1   0/1   0/1   Creating          35s
quick-start   1/1   0/1   1/1   0/1   Creating          93s
quick-start   1/1   0/1   1/1   1/1   Creating          4m43s
quick-start   1/1   1/1   1/1   1/1   Running    2.4 GiB   4m44s
```

当 PHASE 显示为 Running 时，PolarDB-X 集群已经部署完成！恭喜你，现在可以开始连接并体验 PolarDB-X 分布式数据库了！

# 连接 PolarDB-X 集群

PolarDB-X 支持 MySQL 传输协议及绝大多数语法，因此你可以使用 mysql 命令行工具连接 PolarDB-X 进行数据库操作。

在开始之前，请确保已经安装 mysql 命令行工具。

## 转发 PolarDB-X 的访问端口

创建 PolarDB-X 集群时，PolarDB-X Operator 同时会为集群创建用于访问的服务，默认是 ClusterIP 类型。使用下面的命令查看用于访问的服务：

```bash
$ kubectl get svc quick-start
```

期望输出：

```bash
NAME          TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)             AGE
quick-start   ClusterIP   10.110.214.223   <none>        3306/TCP,8081/TCP   5m25s
```

我们使用 kubectl 提供的 port-forward 命名将服务的 3306 端口转发到本地，并且保持转发进程存活。

```bash
$ kubectl port-forward svc/quick-start 3306
```

## 连接 PolarDB-X 集群

Operator 将为 PolarDB-X 集群默认创建一个账号 polardbx_root，并将密码存放在 secret 中。

使用以下命令查看 polardbx_root 账号的密码：

```bash
$ kubectl get secret quick-start -o jsonpath="{.data['polardbx_root']}" | base64 -d - | xargs echo "Password: "
Password:  bvp9wjxx
```

保持 port-forward 的运行，重新打开一个终端，执行如下命令连接集群：

```bash
$ mysql -h127.0.0.1 -P3306 -upolardbx_root -pbvp9wjxx
```

期望输出：

```bash
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 6
Server version: 5.6.29 Tddl Server (ALIBABA)

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

恭喜！你已经成功地部署并连接到了一个 PolarDB-X 分布式数据库集群，现在你可以开始体验分布式数据库的能力了！

# 销毁 PolarDB-X 集群

完成测试后，你可以通过以下命令销毁 PolarDB-X 集群。

```bash
$ kubectl delete polardbxcluster quick-start
```

再次查看以确保删除完成

```bash
$ kubectl get polardbxcluster quick-start
```

# 卸载 PolarDB-X Operator

使用如下命令卸载 PolarDB-X Operator。

```bash
$ helm uninstall --namespace polardbx-operator-system polardbx-operator
```

Helm 卸载并不会删除对应的定制资源 CRD，使用下面的命令查看并删除 PolarDB-X 对应的定制资源：

```bash
$ kubectl get crds | grep polardbx.aliyun.com
polardbxclusters.polardbx.aliyun.com   2021-10-17T07:17:27Z
xstores.polardbx.aliyun.com            2021-10-17T07:17:27Z

$ kubectl delete crds polardbxclusters.polardbx.aliyun.com xstores.polardbx.aliyun.com
```