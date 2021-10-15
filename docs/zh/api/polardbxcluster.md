# polardbx.aliyun.com/v1 PolarDBXCluster

使用 PolarDBXCluster 可以自由定义集群的拓扑、规格和配置，可以支持超大规模和不同容灾等级的部署。

以下是可配置项及相关的字段的含义：

```yaml
apiVersion: polardbx.aliyun.com/v1
kind: PolarDBXCluster
metadata:
  name: full
spec:
  # **Optional**
  #
  # 是否使用 DN-0 作为共享 GMS 以节省资源，默认值 false
  #
  # 不推荐在生产集群使用
  shareGMS: false

  # **Optional**
  #
  # PolarDB-X 集群所支持的 MySQL 协议版本，默认值 5.7
  # 可选值：5.7, 8.0
  protocolVersion: 5.7

  # **Optional**
  #
  # PolarDB-X 集群在 Kubernetes 内对外暴露的服务名，默认为 .metadata.name
  serviceName: full

  # **Optional**
  #
  # PolarDB-X 集群在 Kubernetes 内对外暴露的服务类型，默认为 ClusterIP
  # 可选值参考 Service 的类型
  #
  # 注：云上的 Kubernetes 集群可使用 LoadBalancer 来绑定 LB
  serviceType: LoadBalancer

  # **Optional**
  #
  # PolarDB-X 集群安全配置
  security:
    # **Optional**
    #
    # TLS 相关配置，暂不生效
    tls:
      secretName: tls-secret
    # **Optional**
    #
    # 指定用于编码内部密码的 key，引用指定 Secret 的 key
    encodeKey:
      name: ek-secret
      key: key

  # *Optional**
  #
  # PolarDB-X 初始账号配置
  privileges:
  - username: admin
    password: "123456"
    type: SUPER

  # PolarDB-X 集群配置
  config:
    # CN 相关配置
    cn:
      # 静态配置，修改会导致 CN 集群重建
      static:
        # 启用协程, OpenJDK 暂不支持，需使用 dragonwell
        EnableCoroutine: false
        # 启用备库一致读
        EnableReplicaRead: false
        # 启用 JVM 的远程调试
        EnableJvmRemoteDebug: false
        # 自定义 CN 静态配置，key-value 结构
        ServerProperties:
          processors: 8
      # 动态配置，修改并 apply 会由 operator 自动推送，key-value 结构
      dynamic:
        CONN_POOL_IDLE_TIMEOUT: 30
    # DN 相关配置
    dn:
      # DN my.cnf 配置，覆盖模板部分
      mycnfOverwrite: |-
        loose_binlog_checksum: crc32
      # DN 日志清理间隔
      logPurgeInterval: 5m

  # PolarDB-X 集群拓扑
  topology:
    # 集群使用的镜像版本 (tag)，默认为空（由 operator 指定)
    version: v1.0

    # 集群部署规则
    rules:
      # 预定义节点选择器
      selectors:
      - name: zone-a
        nodeSelector:
          nodeSelectorTerms:
          - matchExpressions:
            - key: topology.kubernetes.io/zone
              operator: In
              values:
              - cn-hangzhou-a
      - name: zone-b
        nodeSelector:
          nodeSelectorTerms:
          - matchExpressions:
            - key: topology.kubernetes.io/zone
              operator: In
              values:
              - cn-hangzhou-b
      - name: zone-c
        nodeSelector:
          nodeSelectorTerms:
          - matchExpressions:
            - key: topology.kubernetes.io/zone
              operator: In
              values:
              - cn-hangzhou-c
      components:
        #  **Optional**
        #
        # GMS 部署规则，默认和 DN 一致
        gms:
          # 堆叠部署结构，operator 尝试在节点选择器指定的节点中，堆叠部署
          # 每个存储节点的子节点以达到较高资源利用率的方式，仅供测试使用
          rolling:
            replicas: 3
            selector:
              reference: zone-a
          # 节点组部署结构，可以指定每个 DN 的子节点的节点组和节点选择器，
          # 从而达成跨区、跨城等高可用部署结构
          nodeSets:
          - name: cand-zone-a
            role: Candidate
            replicas: 1
            selector:
              reference: zone-a
          - name: cand-zone-b
            role: Candidate
            replicas: 1
            selector:
              reference: zone-b
          - name: log-zone-c
            role: Voter
            replicas: 1
            selector:
              reference: zone-c

        # **Optional**
        #
        # DN 部署规则，默认为 3 节点，所有节点可部署
        dn:
          nodeSets:
          - name: cands
            role: Candidate
            replicas: 2
          - name: log
            role: Voter
            replicas: 1

        # **Optional**
        #
        # CN 部署规则，同样按组划分 CN 节点
        cn:
        - name: zone-a
          # 合法值：数字、百分比、(0, 1] 分数，不填写为剩余 replica（只能有一个不填写）
          # 总和不能超过 .topology.nodes.cn.replicas
          replicas: 1
          selector:
            reference: zone-a
        - name: zone-b
          replicas: 1 / 3
          selector:
            reference: zone-b
        - name: zone-c
          replicas: 34%
          selector:
            reference: zone-c

        # **Optional**
        #
        # CDC 部署规则，同 CN
        cdc:
        - name: half
          replicas: 50%
          selector:
            reference: zone-a
        - name: half
          # 带 + 表示向上取整
          replicas: 50%+
          selector:
            reference: zone-b

    nodes:
      # **Optional**
      #
      # GMS 规格配置，默认和 DN 相同
      gms:
        template:
          # 存储节点引擎，默认 galaxy
          engine: galaxy
          # 存储节点镜像，默认由 operator 指定
          image: galaxystore-8:v1.0
          # 存储节点 Service 类型，默认为 ClusterIP
          serviceType: ClusterIP
          # 存储节点 Pod 是否适用宿主机网络，默认为 true
          hostNetwork: true
          # 存储节点磁盘空间限制，不填写无限制（软限制）
          diskQuota: 10Gi
          # 存储节点子节点使用的资源，默认为 4c8g
          resources:
            limits:
              cpu: 4
              memory: 8Gi

      # **Optional**
      #
      # DN 规格配置
      dn:
        # DN 数量配置，默认为 2
        replicas: 2
        template:
          resources:
            limits:
              cpu: 4
              memory: 8Gi
            # IO 相关限制，支持 BPS 和 IOPS 限制
            limits.io:
              iops: 1000
              bps: 10Mi

      # CN 规格配置，参数解释同 DN
      cn:
        replicas: 3
        template:
          image: galaxysql:v1.0
          hostNetwork: false
          resources:
            limits:
              cpu: 4
              memory: 8Gi

      # CDC 规格配置，参数解释同 CN，可不配置代表不启动 CDC 能力
      cdc:
        replicas: 2
        template:
          image: galaxysql-cdc:v1.0
          hostNetwork: false
          resources:
            limits:
              cpu: 4
              memory: 8Gi
```