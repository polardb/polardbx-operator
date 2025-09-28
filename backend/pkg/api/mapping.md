# CRD ↔ Backend API Mapping (Alias Routes)

- Base prefix: `/api/v1/crd/*` (only adds aliases, does not change existing endpoints or request/response)

| CRD Kind | Alias Package | Alias Route | Existing Primary Route (example) |
| --- | --- | --- | --- |
| PolarDBXCluster | `crd/polardbxclusters` | `/api/v1/crd/polardbxclusters` | `/api/v1/clusters` (domain: `/api/v1/polardbxclusters`) |
| SystemTask | `crd/systemtasks` | `/api/v1/crd/systemtasks` | `/api/v1/system-tasks` |
| XStore | `crd/xstores` | `/api/v1/crd/xstores` | `/api/v1/xstores` |
| PolarDBXBackup | `crd/polardbxbackups` | `/api/v1/crd/polardbxbackups` | `/api/v1/backups/*` |
| PolarDBXBackupSchedule | `crd/polardbxbackupschedules` | `/api/v1/crd/polardbxbackupschedules` | `/api/v1/backup-schedules/*` |
| PolarDBXBackupBinlog | `crd/polardbxbackupbinlogs` | `/api/v1/crd/polardbxbackupbinlogs` | `/api/v1/backup-binlogs/*` |
| PolarDBXParameter | `crd/polardbxparameters` | `/api/v1/crd/polardbxparameters` | `/api/v1/parameters/*` |
| PolarDBXParameterTemplate | `crd/polardbxparametertemplates` | `/api/v1/crd/polardbxparametertemplates` | `/api/v1/parameter-templates/*` |
| PolarDBXMonitor | `crd/polardbxmonitors` | `/api/v1/crd/polardbxmonitors` | `/api/v1/monitors/*` |
| PolarDBXLogCollector | `crd/polardbxlogcollectors` | `/api/v1/crd/polardbxlogcollectors` | `/api/v1/log-collectors/*` |

Note: Alias routes are assembled by `api/router`, forwarding to existing handlers; the UI can continue using legacy routes.

---

# Domain Entrances

- Add domain-level entrances only (thin handlers forwarding), do not replace legacy routes:
  - Logical cluster domain: `/api/v1/polardbxclusters/*` (aggregates clusters/backups/backup-schedules/backup-binlogs/parameters/parameter-templates/prechange/restore/cluster-knobs)
  - Storage domain: `/api/v1/xstores/*` (aggregates xstores/xstore-backups/xstore-followers/rebuild)
  - System tasks domain: `/api/v1/systemtasks/*` (aggregates system-tasks)
  - Platform cross-cutting: `/api/v1/platform/*` (aggregates monitoring/grafana/logs/system/pod etc.)

Where assembled: `pkg/api/router` exposes `RegisterCRDAliasRoutes` and `RegisterDomainRoutes`; they are registered during app init, and `LogGroupedRoutes` prints grouped logs for discoverability.

---

# Package Ownership List (for migration guidance, does not affect routes)

- Logical cluster domain `domain/polardbxclusters`
  - Includes: `cluster/` (CRUD/ops wired to services), `backup/`, `backupbinlog/`, `parameters/`, `clusterknobs/`, `prechange/`, `restore/`
- Storage domain `domain/xstores`
  - Includes: `xstore/` (with followers/rebuild and xstore-backups)
- System tasks domain `domain/systemtasks`
  - Includes: `systemtask/`
- Platform cross-cutting `domain/platform`
  - Includes: `monitor/`, `monitoring/`, `grafana/`, `logs/`, `logservice/`, `logstrategy/`, `system/`, `pod/`, `alerts/`, `settings/`, `auth/`

Note: Phase 1 only establishes entrances and skeleton; Phase 2 gradually migrates handlers → domain packages, moves orchestration to `services/`, unifies K8s access in `k8srepo/`, and consolidates constants in `meta/`.
