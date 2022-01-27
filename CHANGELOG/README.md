# Changelog

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

+ Fixes
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