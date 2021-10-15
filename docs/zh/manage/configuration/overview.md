# 参数设置

PolarDB-X 目前支持修改计算节点（CN）的相关参数

## 设置 CN 参数


执行如下命令进入编辑器模式：
```shell
kubectl edit pxc {PolarDB-X 集群名称}
```

在spec.config.cn.dynamic 下面添加对应的参数名和参数值, 保存退出编辑器即可触发参数的自动更新。例如下面给出了修改参数"禁止全表删除/更新"的方式：
```yaml
spec:
  config:
    cn:
      dynamic:
        FORBID_EXECUTE_DML_ALL=FALSE
```
PolarDB-X CN 支持参数列表详见：[计算节点参数](https://help.aliyun.com/document_detail/316576.html)。
