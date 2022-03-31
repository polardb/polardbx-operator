# Changelog

## 2023-03-31

Release v1.2.0.

+ Enhancement & New Features
  + Provide a new CR `PolarDBXMonitor` for declaring the monitoring of some `PolarDBXCluster`. The controller will create `ServiceMonitors` to make prometheus scraping the metrics.
  + Provide a new chart `polardbx-monitor` which packages a customized [kube-prometheus](https://github.com/prometheus-operator/kube-prometheus) release with predefined dashboards for monitoring PolarDB-X clusters.
  + Provide support for xpaxos version of [galaxyengine](https://github.com/ApsaraDB/galaxyengine). Now the `PolarDBXCluster` controller will create a typical paxos cluster (leader + follower + logger) for each GMS and DN by default. 
    + **Note** this is a breaking change. After the upgrade, the old `XStores` will be in an unmaintainable state. Update of the GMS/DN is not possible due to incompatible data/log formats. You may have to delete all the `PolarDBXCluster` in your Kubernetes before/after the upgrade.
    + If you want to keep the compatibility, you can disable this feature by declaring feature gate with negative symbol `EnableGalaxyCluster-`. After that, no multi-node `XStore` with galaxy engine can be created.
  + Support scaling up/down and self-healing of the `XStores`.

+ Bug Fix
  + Fix the behavior of polardbx-exporter when part of the scrape tasks fail.

## 2022-01-27

Release v1.1.0.

+ Enhancement & New Features
  + Support scaling in/out the PolarDBXCluster. 
  + Support configure SSL on PolarDBXCluster.
  + Add a new CRD PolarDBXCluster for setting and reviewing the configs of PolarDB-X cluster.
    + `config.dynamic.CN` is not going to sync with cluster while phase is running.
  + Support admission webhooks for PolarDBXCluster and PolarDBXClusterKnobs. Now the bad specs will be rejected.
  + Support specifying `imagePullPolicy` in component templates.
  + Add label "polardbx/name" to Services、Secrets and ConfigMaps owned by PolarDBXCluster.
  + Support webhooks for PolarDBXCluster and PolarDBXClusterKnobs.
  + Support the complete spec of node selectors in `spec.topology.rules` of PolarDBXCluster.
  + Create headless services for pods of xstore. Record DNS domains instead of raw IP for records of DN in GMS and among galaxyengine xstores. 
  + Support overwrite image tag in values.yaml (helm).
  + Support collect metrics for hotspot JVM 11.
  + Add e2e test tests.

+ Bug Fix
  + Fix the wrong call stack when logging with `flow.Error` in some cases. 
  + Fix the wrong timeout in polardbx-init.
  + Fix configuring host path of data volumes in values.yaml (helm).
  + Fix removing ini keys in galaxyengine's config.
  + Fix a `removeNull` in hsperfdata.

## 2021-10-15 

Release v1.0.0. 

+ Provide v1 APIs:
  + PolarDBXCluster
  + XStore
+ Support deploy operator with Helm 3.