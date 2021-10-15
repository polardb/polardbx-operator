# 连接数据库

PolarDB-X 支持通过 MySQL 命令行、第三方客户端以及符合 MySQL 官方交互协议的第三方程序代码进行连接。本文主要介绍如何通过 MySQL 命令行连接到 PolarDB-X 数据库。


## 前提条件

- 如果您的服务器尚未安装MySQL客户端，请前往[MySQL网站](https://dev.mysql.com/downloads/mysql/)下载安装。
- 您已在 Kubenetes 集群上创建了一个 PolarDB-X 数据库


## 通过MySQL命令行连接到PolarDB-X
### 获取用户名密码
PolarDB-X 默认的 root 账号都是: polardbx_root，您在登录后可以通过[权限管理语句](https://help.aliyun.com/document_detail/313296.html
) 修改密码或者创建新的账号供业务访问。

root 账号的密码随机生成，执行下面的命令获取 PolarDB-X root 账号的密码：

```plsql
kubectl get secret {PolarDB-X 集群名} -o jsonpath="{.data['polardbx_root']}" | base64 -d - | xargs echo "Password: "
```
期望输出：

```plsql
Password:  *******
```
### 通过 ClusterIp 访问
创建 PolarDB-X 集群时，PolarDB-X Operator 同时会为集群创建用于访问的服务，默认是 ClusterIP 类型。使用下面的命令查看用于访问的服务：

```plsql
$ kubectl get svc {PolarDB-X 集群名}
```
期望输出：

```plsql
NAME          TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)             AGE
quick-start   ClusterIP   10.110.214.223   <none>        3306/TCP,8081/TCP   5m25s
```
如果您是在 K8s 集群内进行访问，可以直接使用上面输出的 Cluster-IP 即可。PolarDB-X 服务默认的端口都是 3306.
> ClusterIP 是通过 K8s 集群的内部 IP 暴露服务，选择该访问方式时，只能在集群内部访问

执行如下命令，输入上面获取的密码后，即可连接 PolarDB-X：

```plsql
mysql -h10.110.214.223 -P3306 -upolardbx_root -p
```
> **说明: **
>    - 此处**-P**为大写字母，默认端口为3306。
>    - 为保障密码安全，**-p**后请不要填写密码，会在执行整行命令后提示您输入密码，输入后按回车即可登录。

### 通过 port-forward 转发到本地访问
如果您在 K8s 集群外想访问 PolarDB-X 数据库，但是没有配置 LoadBalancer, 可以通过如下命令将服务的 3306 端口转发到本地，并且保持转发进程存活。

```plsql
kubectl port-forward svc/{PolarDB-X 集群名} 3306
```
> 如果您机器的3306端口被占用，可以通过如下命令将服务转发到指定的端口上：kubectl port-forward svc/{PolarDB-X 集群名} {新端口}:3306

新开一个终端，执行如下命令即可连接 PolarDB-X：

```plsql
mysql -h127.0.0.1 -P{转发端口} -upolardbx_root -p
```
> **说明: **
>    - 此处**-P**为大写字母，默认端口为3306。
>    - 为保障密码安全，**-p**后请不要填写密码，会在执行整行命令后提示您输入密码，输入后按回车即可登录。

### 
### 通过 LoadBalancer 访问

若运行在有 LoadBalancer 的环境，比如阿里云Ack，建议使用云平台的 LoadBalancer 特性。在创建 PolarDB-X 集群时指定 `.spec.serviceType` 为 LoadBalancer
，operator 将会自动创建类型为 LoadBalancer 的服务（Service），此时当云平台支持时 Kubernetes 会自动为该服务配置，如下所示：


```bash
NAME                 TYPE           CLUSTER-IP       EXTERNAL-IP   PORT(S)                         AGE
xxxxxxxxx            LoadBalancer   192.168.247.39   8.209.29.16   3306:30612/TCP,8081:30370/TCP   28h
```

此时可使用 EXTERNAL-IP 所示的 IP 进行访问：

```bash
mysql -h8.209.29.16 -P3306 -upolardbx_root -p
```

## 通过第三方客户端连接到数据库
PolarDB-X支持通过如下第三方客户端进行连接，您可以去对应的官方网站下载客户端。

- MySQL Workbench（推荐）
- SQLyog
- Sequel Pro
- Navicat for MySQL
> **说明** 第三方GUI客户端可执行基础的数据库操作，包括数据的增删改查和DDL操作，对于工具高级特性，PolarDB-X可能并不支持。

## 
## 通过第三方程序代码连接到数据库
PolarDB-X支持通过如下符合MySQL官方交互协议的第三方程序代码进行连接：

- JDBC Driver for MySQL (Connector/J)
- Python Driver for MySQL (Connector/Python)
- C++ Driver for MySQL (Connector/C++)
- C Driver for MySQL (Connector/C)
- ADO.NET Driver for MySQL (Connector/NET)
- ODBC Driver for MySQL (Connector/ODBC)
- PHP Drivers for MySQL (mysqli, ext/mysqli, PDO_MYSQL, PHP_MYSQLND)
- Perl Driver for MySQL (DBD::mysql)
- Ruby Driver for MySQL (ruby-mysql)

以下为JDBC Driver for MySQL （Connector/J）程序代码示例。

```java
//JDBC 
Class.forName("com.mysql.jdbc.Driver");  
Connection conn = DriverManager.getConnection("jdbc:mysql://{ip}:{port}/doc_test","polardbx_root","password"); 
//... 
conn.close();  
```
以下为应用端连接池配置示例。

```xml
<bean id="dataSource" class="com.alibaba.druid.pool.DruidDataSource" init-method="init" destroy-method="close"> 
    <property name="url" value="jdbc:mysql://pxc-******************.public.polarx.rds.aliyuncs.com:3306/doc_test" />
    <property name="username" value="doc_test" />
    <property name="password" value="doc_test_password" />
    <property name="filters" value="stat" />
    <property name="maxActive" value="100" />
    <property name="initialSize" value="20" />
    <property name="maxWait" value="60000" />
    <property name="minIdle" value="1" />
    <property name="timeBetweenEvictionRunsMillis" value="60000" />
    <property name="minEvictableIdleTimeMillis" value="300000" />
    <property name="testWhileIdle" value="true" />
    <property name="testOnBorrow" value="false" />
    <property name="testOnReturn" value="false" />
    <property name="poolPreparedStatements" value="true" />
    <property name="maxOpenPreparedStatements" value="20" />
    <property name="asyncInit" value="true" />
</bean>
```
