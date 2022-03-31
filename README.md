GalaxyKube -- PolarDB-X Operator

---

PolarDB-X Operator is a Kubernetes extension that aims to create and manage PolarDB-X cluster on Kubernetes. It follows
the [operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) and automates the management
tasks.

## Quick Start

Follow the [[Quick Start](./docs/en/deploy/quick-start.md) / [快速开始](./docs/zh/deploy/quick-start.md)] guide to start a testing Kubernetes cluster and play with PolarDB-X Operator.

## Documentation

**NOTE: v1.2.0 introduces a breaking change to support the new xpaxos galaxyengine. Please refer to the [CHANGELOG](./CHANGELOG.md) for upgrade guidance.**

Refer to the documentations for more details, such as CRD definitions and operation guides.

+ [简体中文](docs/zh/index.md)
+ [English](docs/en/index.md)

## Roadmap

There are many aspects of management activities. For now, only some are implemented. Here's a roadmap describing
what features we have implemented and what we are going to implement and their (possible) priorities.

Implemented:

+ ✅ Basic Lifetime Management
+ ✅ Liveness/Readiness Probing & Self Healing
+ ✅ Metrics Export and Grafana/Prometheus Integration
+ ✅ Dynamic Configuration
+ ✅ Scale and Upgrade
+ ✅ Automated Data Rebalance
+ ✅ Automated Failover
+ ✅ Security and TLS
+ ✅ Multiple Architecture Support
  + linux/amd64, linux/arm64

Working in progress:

+ [T0] Backup and Restore 
+ [T1] Backup Plan
+ [T1] Configuration (DN) 
+ [T2] Dashboard
+ ...

## License

PolarDB-X operator is distributed under the Apache License (Version 2.0). See the [LICENSE](./LICENSE) file for details.

This product contains various third-party components under other open source licenses.
See the [NOTICE](./NOTICE.md) file for more information.
