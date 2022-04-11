# PolarDB-X Operator 安装指南

PolarDB-X Operator 通过 Helm 包管理器进行安装，安装命令如下所示

```bash
$ kubectl create namespace polardbx-operator-system

$ helm install --namespace polardbx-operator-system polardbx-operator https://github.com/ApsaraDB/galaxykube/releases/download/v1.2.1/polardbx-operator-1.2.0.tgz
```

Helm Chart 允许在安装时支持自定义参数值，可配置项参考 [charts/polardbx-operator/values.yaml](/charts/polardbx-operator/values.yaml) 。

例，当想要使用 latest 镜像来部署 operator 、并且使用宿主机上的 /vol/data 作为数据目录时，可以编写如下 values.yaml：

```yaml
useLatestImage: true
node:
  volumes:
    data: /vol/data
```

然后指定 values.yaml 安装：

```bash
helm install -f /path/to/values.yaml --namespace polardbx-operator-system polardbx-operator https://github.com/ApsaraDB/galaxykube/releases/download/v1.2.1/polardbx-operator-1.2.0.tgz
```
