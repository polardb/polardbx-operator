import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders, HttpErrorResponse, HttpParams } from '@angular/common/http';
import { Observable, throwError, of } from 'rxjs';
import { catchError, finalize, tap, map } from 'rxjs/operators';
import { PolarDBXCluster } from '../models/cluster.model';
import { ClusterCreationConfig } from '../models/cluster-creation.model';
import { Pod } from '../models/pod.model';
import { PolarDBXBackup, BackupInfo, CreateBackupRequest } from '../models/backup.model';
import { XStore, CreateXStoreRequest } from '../models/xstore.model';
import { PolarDBXMonitor, CreateMonitorRequest } from '../models/monitor.model';
import { PolarDBXBackupSchedule, CreateBackupScheduleRequest } from '../models/backup-schedule.model';
import { PolarDBXParameterTemplate, CreateParameterTemplateRequest } from '../models/parameter-template.model';
import { SystemTask, SystemTaskList, CreateSystemTaskRequest, UpdateSystemTaskRequest } from '../models/system-task.model';
import { PolarDBXLogCollector, CreateLogCollectorRequest } from '../models/log-collector.model';
import { XStoreFollower, CreateXStoreFollowerRequest } from '../models/xstore-follower.model';
import { XStoreBackup, CreateXStoreBackupRequest } from '../models/xstore-backup.model';
import { PolarDBXClusterKnobs, PolarDBXClusterKnobsList, CreateClusterKnobsRequest } from '../models/cluster-knobs.model';
import { RestoreClusterRequest, PITRRequest, RestoreStatusResponse, RestoreJob, RestoreResponse, PITRResponse, CancelRestoreResponse } from '../models/restore.model';
import { PolarDBXBackupBinlog, CreateBackupBinlogRequest, UpdateBackupBinlogRequest } from '../models/backup-binlog.model';
import { GrafanaTemplateSummary, GrafanaTemplateDetail } from '../models/monitoring-dashboard.model';
import { AlertRuleTemplateSummary, AlertRuleTemplateDetail, ApplyAlertRuleTemplatePayload, ApplyAlertRuleTemplateResponse } from '../models/monitoring-alert-template.model';
import { ClusterResourceUsage } from '../models/cluster-resource-usage.model';
import { ErrorHandlerService } from './error-handler.service';
import { LoadingService, LoadingKeys } from './loading.service';
import { PerformanceService } from './performance.service';
import { LogsPresetList, LogsQueryRequest, NormalizedResponse } from '../models/logs.model';
import { appendKubeconfigHeader, buildJsonHeaders } from '../utils/http-headers';

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private http = inject(HttpClient);
  private errorHandler = inject(ErrorHandlerService);
  private loadingService = inject(LoadingService);
  private performanceService = inject(PerformanceService);

  // Use relative path to avoid hardcoding localhost
  // Production deployment will use /api/v1 on the same domain
  private baseUrl = '/api/v1';

  private getHeaders(): HttpHeaders {
    return buildJsonHeaders();
  }

  private withNs(params?: HttpParams): HttpParams {
    const base = params || new HttpParams();
    const stored = (localStorage.getItem('activeNamespace') || '').trim();
    if (stored) {
      return base.set('namespace', stored);
    }
    return base;
  }

  // Validate connection
  connect(kubeconfig: string): Observable<unknown> {
    // Use safer base64 encoding method to handle Unicode characters
    const kubeconfigB64 = btoa(unescape(encodeURIComponent(kubeconfig)));
    const headers = appendKubeconfigHeader(new HttpHeaders({ 'Content-Type': 'application/json' })).set('X-Kubeconfig-B64', kubeconfigB64);
    return this.handleRequest(
      this.http.post(`${this.baseUrl}/connect`, {}, { headers }),
      LoadingKeys.CONNECT,
      '/connect',
      'POST'
    );
  }

  // Get cluster list
  getClusters(namespace?: string, opts?: { silent?: boolean }): Observable<PolarDBXCluster[]> {
    const params = namespace ? new HttpParams().set('namespace', namespace) : this.withNs();
    return this.handleRequest(
      this.http.get<PolarDBXCluster[]>(`${this.baseUrl}/clusters`, {
        headers: this.getHeaders(),
        params
      }),
      LoadingKeys.CLUSTERS_LIST,
      '/clusters',
      'GET',
      opts
    );
  }

  // Get single cluster
  getCluster(namespace: string, name: string): Observable<PolarDBXCluster> {
    return this.handleRequest(
      this.http.get<PolarDBXCluster>(`${this.baseUrl}/clusters/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_DETAIL,
      `/clusters/${namespace}/${name}`,
      'GET'
    );
  }

  // Get aggregated CPU/memory usage for cluster (engine/server only; metrics-server required).
  getClusterResourceUsage(namespace: string, name: string, silent = true): Observable<ClusterResourceUsage> {
    return this.handleRequest(
      this.http.get<ClusterResourceUsage>(`${this.baseUrl}/clusters/${namespace}/${name}/resource-usage`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_RESOURCE_USAGE,
      `/clusters/${namespace}/${name}/resource-usage`,
      'GET',
      { silent }
    );
  }

  // Get alert summary (Alertmanager or Events fallback)
  getClusterAlertsSummary(namespace: string, name: string, alertmanager?: string): Observable<{critical:number;warning:number;info:number;total:number;source:string}> {
    const params = alertmanager ? `?alertmanager=${encodeURIComponent(alertmanager)}` : '';
    return this.handleRequest(
      this.http.get<{critical:number;warning:number;info:number;total:number;source:string}>(`${this.baseUrl}/clusters/${namespace}/${name}/alerts-summary${params}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_DETAIL,
      `/clusters/${namespace}/${name}/alerts-summary`,
      'GET'
    );
  }

  // Get Pod list for cluster
  getPodsForCluster(namespace: string, name: string): Observable<Pod[]> {
    return this.handleRequest(
      this.http.get<Pod[]>(`${this.baseUrl}/clusters/${namespace}/${name}/pods`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.PODS_LIST,
      `/clusters/${namespace}/${name}/pods`,
      'GET'
    );
  }

  // List Pods by namespace (not limited to cluster)
  listPods(namespace = 'default'): Observable<Pod[]> {
    return this.handleRequest(
      this.http.get<Pod[]>(`${this.baseUrl}/pods`, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.PODS_LIST,
      `/pods`,
      'GET'
    );
  }

  // Get Pod logs
  getPodLogs(namespace: string, podName: string, containerName: string, tailLines = 1000): Observable<string> {
    const headers = this.getHeaders().set('Accept', 'text/plain');
    return this.handleRequest(
      this.http.get(`${this.baseUrl}/logs/${namespace}/${podName}?container=${containerName}&tailLines=${tailLines}`, {
        headers: headers,
        responseType: 'text'
      }),
      LoadingKeys.POD_LOGS,
      `/logs/${namespace}/${podName}`,
      'GET'
    );
  }

  // Get single Pod details
  getPod(namespace: string, podName: string): Observable<Pod> {
    return this.handleRequest(
      this.http.get<Pod>(`${this.baseUrl}/pods/${encodeURIComponent(namespace)}/${encodeURIComponent(podName)}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.POD_DETAIL,
      `/pods/${namespace}/${podName}`,
      'GET'
    );
  }

  // Delete Pod (for node restart/rebuild)
  deletePod(namespace: string, podName: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/pods/${encodeURIComponent(namespace)}/${encodeURIComponent(podName)}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.POD_DELETE,
      `/pods/${namespace}/${podName}`,
      'DELETE'
    );
  }

  // Execute one-time command (non-interactive), returns stdout text
  execPod(namespace: string, podName: string, containerName: string, cmd: string, tty: boolean = true): Observable<string> {
    const headers = this.getHeaders().set('Accept', 'text/plain');
    const url = `${this.baseUrl}/pods/${encodeURIComponent(namespace)}/${encodeURIComponent(podName)}/exec?container=${encodeURIComponent(containerName || '')}&cmd=${encodeURIComponent(cmd)}&tty=${tty}`;
    return this.handleRequest(
      this.http.get(url, { headers, responseType: 'text' }),
      LoadingKeys.POD_LOGS,
      `/pods/${namespace}/${podName}/exec`,
      'GET'
    );
  }

  // Get backup list
  getBackups(namespace: string, clusterName: string): Observable<PolarDBXBackup[]> {
    return this.handleRequest(
      this.http.get<PolarDBXBackup[]>(`${this.baseUrl}/clusters/${namespace}/${clusterName}/backups`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUPS_LIST,
      `/clusters/${namespace}/${clusterName}/backups`,
      'GET'
    );
  }

  // Get backup aggregated metrics (coarse-grained progress and child backup stats)
  getBackupMetrics(namespace: string, name: string, opts?: { silent?: boolean }): Observable<{ phase: string; progress: number; estimated: boolean; sizeBytes?: number; sizeEstimated?: boolean; children?: { total: number; finished: number; failed: number } } & any> {
    return this.handleRequest(
      this.http.get<{ phase: string; progress: number; estimated: boolean; sizeBytes?: number; sizeEstimated?: boolean; children?: { total: number; finished: number; failed: number } } & any>(
        `${this.baseUrl}/backups/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/metrics`,
        { headers: this.getHeaders() }
      ),
      LoadingKeys.BACKUPS_LIST,
      `/backups/${namespace}/${name}/metrics`,
      'GET',
      opts
    );
  }

  // Backup download info (url + suggested commands)
  getBackupDownloadInfo(namespace: string, name: string): Observable<{ namespace: string; name: string; phase: string; backupRootPath: string; storage: string; sink: string; sinkConfig?: any; url: string; filename: string; message?: string; command?: string } & any> {
    return this.handleRequest(
      this.http.get<{ namespace: string; name: string; phase: string; backupRootPath: string; storage: string; sink: string; sinkConfig?: any; url: string; filename: string; message?: string; command?: string } & any>(
        `${this.baseUrl}/backups/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/download`,
        { headers: this.getHeaders() }
      ),
      LoadingKeys.BACKUPS_LIST,
      `/backups/${namespace}/${name}/download`,
      'GET'
    );
  }

  // Backup file download (tar.gz streaming)
  downloadBackupFile(namespace: string, name: string): Observable<Blob> {
    const headers = this.getHeaders().set('Accept', 'application/gzip');
    return this.handleRequest(
      this.http.get(`${this.baseUrl}/backups/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/file`, {
        headers,
        responseType: 'blob'
      }),
      LoadingKeys.BACKUPS_LIST,
      `/backups/${namespace}/${name}/file`,
      'GET'
    );
  }

  // Get backup role advice (leader/follower)
  getBackupAdvice(namespace: string, clusterName: string): Observable<{ hasFollower: boolean; role: 'leader' | 'follower'; reason?: string }> {
    return this.handleRequest(
      this.http.get<{ hasFollower: boolean; role: 'leader' | 'follower'; reason?: string }>(
        `${this.baseUrl}/clusters/${encodeURIComponent(namespace)}/${encodeURIComponent(clusterName)}/backup-advice`,
        { headers: this.getHeaders() }
      ),
      LoadingKeys.BACKUP_CREATE,
      `/clusters/${namespace}/${clusterName}/backup-advice`,
      'GET'
    );
  }

  // Validate if sink exists
  validateSink(name: string, type: string): Observable<{ name: string; type: string; status: string; message?: string }> {
    return this.handleRequest(
      this.http.post<{ name: string; type: string; status: string; message?: string }>(
        `${this.baseUrl}/hpfs/sinks/validate`,
        { name, type },
        { headers: this.getHeaders() }
      ),
      LoadingKeys.BACKUP_CREATE,
      `/hpfs/sinks/validate`,
      'POST'
    );
  }

  // Dry-run validation (does not persist)
  validateBackup(namespace: string, backupObject: any): Observable<{ valid: boolean }> {
    return this.handleRequest(
      this.http.post<{ valid: boolean }>(`${this.baseUrl}/backups/validate`, backupObject, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.BACKUPS_LIST,
      `/backups/validate`,
      'POST'
    );
  }

  // Create backup
  createBackup(namespace: string, clusterName: string, backupObject: any): Observable<PolarDBXBackup> {
    const headers = this.getHeaders();
    const url = `${this.baseUrl}/clusters/${namespace}/${clusterName}/backups`;

    // Generate DNS-1123 compliant backup name, limit 63 characters
    const generateName = (baseClusterName: string, suffixHint?: string): string => {
      const now = new Date();
      const ts = `${now.getFullYear()}${(now.getMonth() + 1).toString().padStart(2, '0')}${now.getDate()
        .toString().padStart(2, '0')}-${now.getHours().toString().padStart(2, '0')}${now.getMinutes()
        .toString().padStart(2, '0')}${now.getSeconds().toString().padStart(2, '0')}`;
      const rand = Math.random().toString(36).slice(2, 6);
      const suffix = suffixHint ? `-${suffixHint}` : '';
      const raw = `${baseClusterName}-bak-${ts}${suffix}-${rand}`.toLowerCase();
      const sanitized = raw.replace(/[^a-z0-9-]/g, '-');
      if (sanitized.length <= 63) return sanitized;
      const over = sanitized.length - 63;
      // Truncate clusterName part, preserve suffix structure
      const keep = Math.max(1, baseClusterName.length - over);
      const truncated = `${baseClusterName.slice(0, keep)}-bak-${ts}${suffix}-${rand}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');
      return truncated.slice(0, 63);
    };

    const cloneDeep = (obj: any) => JSON.parse(JSON.stringify(obj || {}));

    // Ensure metadata/name exists
    const initialBody = cloneDeep(backupObject);
    initialBody.metadata = initialBody.metadata || {};
    if (!initialBody.metadata.name || typeof initialBody.metadata.name !== 'string') {
      initialBody.metadata.name = generateName(clusterName);
    }

    const postOnce = (body: any) => this.http.post<PolarDBXBackup>(url, body, { headers });

    // First request, if 409 conflict occurs, automatically rename and retry once
    const request$ = postOnce(initialBody).pipe(
      catchError((error: HttpErrorResponse) => {
        if (error && error.status === 409) {
          const retryBody = cloneDeep(initialBody);
          retryBody.metadata = retryBody.metadata || {};
          retryBody.metadata.name = generateName(clusterName, 'auto');
          return postOnce(retryBody);
        }
        return throwError(() => error);
      })
    );

    return this.handleRequest(
      request$,
      LoadingKeys.BACKUP_CREATE,
      `/clusters/${namespace}/${clusterName}/backups`,
      'POST'
    );
  }

  // Delete backup
  deleteBackup(namespace: string, backupName: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/backups/${namespace}/${backupName}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_DELETE,
      `/backups/${namespace}/${backupName}`,
      'DELETE'
    );
  }

  // Force delete backup (remove finalizers)
  forceDeleteBackup(namespace: string, backupName: string): Observable<any> {
    return this.handleRequest(
      this.http.post(`${this.baseUrl}/backups/${namespace}/${backupName}/force-delete`, {}, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_DELETE,
      `/backups/${namespace}/${backupName}/force-delete`,
      'POST'
    );
  }

  // Create cluster (raw API, directly accepts PolarDBXCluster object)
  createCluster(cluster: PolarDBXCluster): Observable<PolarDBXCluster> {
    return this.handleRequest(
      this.http.post<PolarDBXCluster>(`${this.baseUrl}/clusters`, cluster, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_CREATE,
      '/clusters',
      'POST'
    );
  }

  // Create cluster from user-friendly configuration
  createClusterFromConfig(namespace: string, config: ClusterCreationConfig): Observable<PolarDBXCluster> {
    return this.handleRequest(
      this.http.post<PolarDBXCluster>(`${this.baseUrl}/clusters/${namespace}/create`, config, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_CREATE,
      `/clusters/${namespace}/create`,
      'POST'
    );
  }

  // Update cluster log configuration
  updateClusterLogConfig(namespace: string, clusterName: string, nodeType: string, config: any): Observable<any> {
    return this.handleRequest(
      this.http.patch<any>(`${this.baseUrl}/clusters/${namespace}/${clusterName}/log-config/${nodeType}`, config, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_UPDATE,
      `/clusters/${namespace}/${clusterName}/log-config/${nodeType}`,
      'PATCH'
    );
  }

  // Scale cluster
  scaleCluster(namespace: string, clusterName: string, scaling: any, precheckToken?: string, precheckSig?: string): Observable<any> {
    let headers = this.getHeaders();
    if (precheckToken) headers = headers.set('X-Precheck-Token', precheckToken);
    if (precheckSig) headers = headers.set('X-Precheck-Token-Signature', precheckSig);
    return this.handleRequest(
      this.http.patch<any>(`${this.baseUrl}/clusters/${namespace}/${clusterName}/scale`, scaling, {
        headers
      }),
      LoadingKeys.CLUSTER_UPDATE,
      `/clusters/${namespace}/${clusterName}/scale`,
      'PATCH'
    );
  }

  // Upgrade cluster
  upgradeCluster(namespace: string, clusterName: string, upgrade: any, precheckToken?: string, precheckSig?: string): Observable<any> {
    let headers = this.getHeaders();
    if (precheckToken) headers = headers.set('X-Precheck-Token', precheckToken);
    if (precheckSig) headers = headers.set('X-Precheck-Token-Signature', precheckSig);
    return this.handleRequest(
      this.http.patch<any>(`${this.baseUrl}/clusters/${namespace}/${clusterName}/upgrade`, upgrade, {
        headers
      }),
      LoadingKeys.CLUSTER_UPDATE,
      `/clusters/${namespace}/${clusterName}/upgrade`,
      'PATCH'
    );
  }

  // Get cluster upgrade plan (candidate versions/compatibility matrix/recommendations), returns fallback data when backend unavailable
  getClusterUpgradePlan(namespace: string, clusterName: string): Observable<{ currentVersion: string; candidates: Array<{ version: string; recommended?: boolean; notes?: string }>; matrix?: any }> {
    const url = `${this.baseUrl}/clusters/${encodeURIComponent(namespace)}/${encodeURIComponent(clusterName)}/upgrade-plan`;
    type UpgradePlan = { currentVersion: string; candidates: Array<{ version: string; recommended?: boolean; notes?: string }>; matrix?: any };
    const req$: Observable<UpgradePlan> = this.http.get<any>(url, { headers: this.getHeaders() }).pipe(
      map((res: any): UpgradePlan => {
        const current = res?.currentVersion || res?.current || '';
        const cands = Array.isArray(res?.candidates) ? res.candidates : [];
        if (cands.length > 0) return { currentVersion: current, candidates: cands, matrix: res?.matrix };
        // If interface exists but no candidates, fallback to default
        return {
          currentVersion: current,
          candidates: [
            { version: '5.4.19', recommended: true },
            { version: '5.4.18' }
          ]
        } as UpgradePlan;
      }),
      catchError((_err: any) => {
        const cur = (localStorage.getItem('lastClusterVersion') || '').trim();
        const fallback: UpgradePlan = {
          currentVersion: cur,
          candidates: [
            { version: '5.4.19', recommended: true },
            { version: '5.4.18' }
          ]
        };
        return of<UpgradePlan>(fallback);
      })
    );
    return this.handleRequest<UpgradePlan>(
      req$,
      LoadingKeys.CLUSTER_DETAIL,
      `/clusters/${namespace}/${clusterName}/upgrade-plan`,
      'GET'
    );
  }

  // Update cluster
  updateCluster(namespace: string, name: string, cluster: PolarDBXCluster): Observable<PolarDBXCluster> {
    return this.handleRequest(
      this.http.put<PolarDBXCluster>(`${this.baseUrl}/clusters/${namespace}/${name}`, cluster, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_UPDATE,
      `/clusters/${namespace}/${name}`,
      'PUT'
    );
  }

  // Delete cluster
  deleteCluster(namespace: string, name: string): Observable<unknown> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/clusters/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_DELETE,
      `/clusters/${namespace}/${name}`,
      'DELETE'
    );
  }

  /**
   * Test connection
   */
  testConnection(): Observable<unknown> {
    return this.handleRequest(
      this.http.get(`${this.baseUrl}/health`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CONNECT,
      '/health',
      'GET'
    );
  }

  private handleRequest<T>(request: Observable<T>, loadingKey: string, endpoint = '', method = 'GET', opts?: { silent?: boolean }): Observable<T> {
    // Only set global overlay (GLOBAL) for critical operations, regular detail/list requests only record performance without blocking page
    const shouldShowGlobal = (loadingKey as any) === LoadingKeys.CONNECT;
    // Defense: only set global overlay for CONNECT
    if (shouldShowGlobal) this.loadingService.setLoading(LoadingKeys.GLOBAL, true);
    this.loadingService.setLoading(loadingKey, true);
    const startTime = performance.now();
    
    return request.pipe(
      tap(() => {
        // Record successful API performance
        const duration = performance.now() - startTime;
        this.performanceService.recordApiPerformance(endpoint, method, duration, 200);
      }),
      catchError((error: HttpErrorResponse) => {
        // Record failed API performance
        const duration = performance.now() - startTime;
        this.performanceService.recordApiPerformance(endpoint, method, duration, error.status);
        this.performanceService.recordError();
        if (!opts?.silent) {
          this.errorHandler.handleHttpError(error, '请求失败');
        }
        return throwError(() => error);
      }),
      finalize(() => {
        this.loadingService.setLoading(loadingKey, false);
        if (shouldShowGlobal) this.loadingService.setLoading(LoadingKeys.GLOBAL, false);
      })
    );
  }

  // XStore Management Methods

  getXStores(namespace?: string): Observable<XStore[]> {
    const params = namespace ? new HttpParams().set('namespace', namespace) : this.withNs();
    return this.handleRequest(
      this.http.get<XStore[]>(`${this.baseUrl}/xstores`, {
        headers: this.getHeaders(),
        params
      }),
      LoadingKeys.XSTORE_LIST,
      `/xstores`,
      'GET'
    );
  }

  createXStore(namespace: string, xstoreRequest: CreateXStoreRequest): Observable<XStore> {
    const limitsCpu = xstoreRequest.resources?.limits?.['cpu'] || '2';
    const limitsMemory = xstoreRequest.resources?.limits?.['memory'] || '4Gi';
    const diskQuota = xstoreRequest.storage?.size || xstoreRequest.diskQuota || '100Gi';
    const storageClass = xstoreRequest.storage?.storageClass || xstoreRequest.storageClass;
    const cnReplicas = typeof xstoreRequest.cnReplicas === 'number' ? xstoreRequest.cnReplicas : 0;
    const serviceType = xstoreRequest.serviceType || 'NodePort';
    const engine = xstoreRequest.engine || 'galaxy';
    const nodeCount = Math.max(1, Number(xstoreRequest.nodeCount || 2));

    const body: any = {
      apiVersion: 'polardbx.aliyun.com/v1',
      kind: 'XStore',
      metadata: { name: xstoreRequest.name, namespace },
      spec: {
        engine: engine,
        topology: {
          nodes: {
            // Minimal DN-only topology; CN set to 0 to avoid creating SQL service nodes here
            dn: {
              replicas: nodeCount,
              template: {
                hostNetwork: true,
                resources: { limits: { cpu: limitsCpu, memory: limitsMemory } },
                diskQuota: diskQuota
              }
            },
            cn: { replicas: cnReplicas, template: { resources: { limits: { cpu: limitsCpu, memory: limitsMemory } } } },
            gms: { template: { serviceType: serviceType } }
          }
        },
        config: {
          controller: { RPCProtocolVersion: "2" }
        }
      }
    };
    if (storageClass) {
      // When storageClass provided, attach via template volumes hint (hostPath is omitted for PVC-based)
      // Depending on CRD, storage class is usually picked up from StorageClass on PVC; leaving as is for operator defaults.
    }
    const cleaned = JSON.parse(JSON.stringify(body));
    const params = new HttpParams().set('namespace', namespace || xstoreRequest.namespace || 'default');
    return this.handleRequest(
      this.http.post<XStore>(`${this.baseUrl}/xstores`, body, {
        headers: this.getHeaders(),
        params
      }),
      LoadingKeys.XSTORE_CREATE,
      `/xstores`,
      'POST'
    );
  }

  getXStore(namespace: string, name: string): Observable<XStore> {
    return this.handleRequest(
      this.http.get<XStore>(`${this.baseUrl}/xstores/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_DETAIL,
      `/xstores/${namespace}/${name}`,
      'GET'
    );
  }

  // List Pods under an XStore (for target Pod dropdown)
  getXStorePods(namespace: string, xstoreName: string): Observable<Pod[]> {
    const url = `${this.baseUrl}/xstores/${encodeURIComponent(namespace)}/${encodeURIComponent(xstoreName)}/pods`;
    return this.handleRequest(
      this.http.get<Pod[]>(url, { headers: this.getHeaders() }),
      LoadingKeys.PODS_LIST,
      `/xstores/${namespace}/${xstoreName}/pods`,
      'GET'
    );
  }

  updateXStore(namespace: string, xstore: XStore): Observable<XStore> {
    return this.handleRequest(
      this.http.put<XStore>(`${this.baseUrl}/xstores/${namespace}/${xstore.metadata.name}`, xstore, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_UPDATE,
      `/xstores/${namespace}/${xstore.metadata.name}`,
      'PUT'
    );
  }

  deleteXStore(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/xstores/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_DELETE,
      `/xstores/${namespace}/${name}`,
      'DELETE'
    );
  }

  // Monitor Management Methods

  getMonitors(namespace = 'default'): Observable<PolarDBXMonitor[]> {
    return this.handleRequest(
      this.http.get<PolarDBXMonitor[]>(`${this.baseUrl}/monitors`, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.MONITOR_LIST,
      `/monitors`,
      'GET'
    );
  }

  createMonitor(namespace: string, monitorRequest: CreateMonitorRequest): Observable<PolarDBXMonitor> {
    return this.handleRequest(
      this.http.post<PolarDBXMonitor>(`${this.baseUrl}/monitors`, monitorRequest, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.MONITOR_CREATE,
      `/monitors`,
      'POST'
    );
  }

  getMonitor(namespace: string, name: string): Observable<PolarDBXMonitor> {
    return this.handleRequest(
      this.http.get<PolarDBXMonitor>(`${this.baseUrl}/monitors/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.MONITOR_DETAIL,
      `/monitors/${namespace}/${name}`,
      'GET'
    );
  }

  updateMonitor(namespace: string, monitor: PolarDBXMonitor): Observable<PolarDBXMonitor> {
    return this.handleRequest(
      this.http.put<PolarDBXMonitor>(`${this.baseUrl}/monitors/${namespace}/${monitor.metadata.name}`, monitor, {
        headers: this.getHeaders()
      }),
      LoadingKeys.MONITOR_UPDATE,
      `/monitors/${namespace}/${monitor.metadata.name}`,
      'PUT'
    );
  }

  deleteMonitor(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/monitors/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.MONITOR_DELETE,
      `/monitors/${namespace}/${name}`,
      'DELETE'
    );
  }

  // PolarDBXBackupSchedule Management Methods

  getBackupSchedules(namespace = 'default'): Observable<PolarDBXBackupSchedule[]> {
    return this.handleRequest(
      this.http.get<PolarDBXBackupSchedule[]>(`${this.baseUrl}/backup-schedules`, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.BACKUP_SCHEDULE_LIST,
      `/backup-schedules`,
      'GET'
    );
  }

  createBackupSchedule(namespace: string, scheduleRequest: CreateBackupScheduleRequest): Observable<PolarDBXBackupSchedule> {
    const body: any = {
      apiVersion: 'polardbx.aliyun.com/v1',
      kind: 'PolarDBXBackupSchedule',
      metadata: { name: scheduleRequest.name, namespace },
      spec: {
        schedule: scheduleRequest.schedule,
        suspend: !!scheduleRequest.suspend,
        maxBackupCount: scheduleRequest.maxBackupCount,
        backupSpec: {
          cluster: { name: scheduleRequest.clusterName },
          retentionTime: scheduleRequest.retentionTime ? `${Math.floor((scheduleRequest.retentionTime.duration||0)/3600000000000)}h` : undefined,
          cleanPolicy: scheduleRequest.cleanPolicy,
          storageProvider: scheduleRequest.storageProvider,
          preferredBackupRole: scheduleRequest.preferredBackupRole
        }
      }
    };
    // Remove undefined fields
    const cleaned = JSON.parse(JSON.stringify(body));
    return this.handleRequest(
      this.http.post<PolarDBXBackupSchedule>(`${this.baseUrl}/backup-schedules`, cleaned, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.BACKUP_SCHEDULE_CREATE,
      `/backup-schedules`,
      'POST'
    );
  }

  getBackupSchedule(namespace: string, name: string): Observable<PolarDBXBackupSchedule> {
    return this.handleRequest(
      this.http.get<PolarDBXBackupSchedule>(`${this.baseUrl}/backup-schedules/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_SCHEDULE_DETAIL,
      `/backup-schedules/${namespace}/${name}`,
      'GET'
    );
  }

  updateBackupSchedule(namespace: string, schedule: PolarDBXBackupSchedule): Observable<PolarDBXBackupSchedule> {
    return this.handleRequest(
      this.http.put<PolarDBXBackupSchedule>(`${this.baseUrl}/backup-schedules/${namespace}/${schedule.metadata.name}`, schedule, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_SCHEDULE_UPDATE,
      `/backup-schedules/${namespace}/${schedule.metadata.name}`,
      'PUT'
    );
  }

  deleteBackupSchedule(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/backup-schedules/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_SCHEDULE_DELETE,
      `/backup-schedules/${namespace}/${name}`,
      'DELETE'
    );
  }

  // Parameter Template
  getParameterTemplates(namespace = 'default'): Observable<PolarDBXParameterTemplate[]> {
    return this.handleRequest(
      this.http.get<PolarDBXParameterTemplate[]>(`${this.baseUrl}/parameter-templates`, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.PARAMETER_TEMPLATE_LIST,
      `/parameter-templates`,
      'GET'
    );
  }

  createParameterTemplate(namespace: string, templateRequest: CreateParameterTemplateRequest): Observable<PolarDBXParameterTemplate> {
    return this.handleRequest(
      this.http.post<PolarDBXParameterTemplate>(`${this.baseUrl}/parameter-templates`, templateRequest, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.PARAMETER_TEMPLATE_CREATE,
      `/parameter-templates`,
      'POST'
    );
  }

  getParameterTemplate(namespace: string, name: string): Observable<PolarDBXParameterTemplate> {
    return this.handleRequest(
      this.http.get<PolarDBXParameterTemplate>(`${this.baseUrl}/parameter-templates/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.PARAMETER_TEMPLATE_DETAIL,
      `/parameter-templates/${namespace}/${name}`,
      'GET'
    );
  }

  updateParameterTemplate(namespace: string, template: PolarDBXParameterTemplate): Observable<PolarDBXParameterTemplate> {
    return this.handleRequest(
      this.http.put<PolarDBXParameterTemplate>(`${this.baseUrl}/parameter-templates/${namespace}/${template.metadata.name}`, template, {
        headers: this.getHeaders()
      }),
      LoadingKeys.PARAMETER_TEMPLATE_UPDATE,
      `/parameter-templates/${namespace}/${template.metadata.name}`,
      'PUT'
    );
  }

  deleteParameterTemplate(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/parameter-templates/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.PARAMETER_TEMPLATE_DELETE,
      `/parameter-templates/${namespace}/${name}`,
      'DELETE'
    );
  }

  // System Tasks
  getSystemTasks(namespace = 'default'): Observable<SystemTaskList> {
    return this.handleRequest(
      this.http.get<SystemTaskList>(`${this.baseUrl}/system-tasks`, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.SYSTEM_TASK_LIST,
      `/system-tasks`,
      'GET'
    );
  }

  createSystemTask(namespace: string, taskRequest: CreateSystemTaskRequest): Observable<SystemTask> {
    return this.handleRequest(
      this.http.post<SystemTask>(`${this.baseUrl}/system-tasks`, taskRequest, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.SYSTEM_TASK_CREATE,
      `/system-tasks`,
      'POST'
    );
  }

  getSystemTask(namespace: string, name: string): Observable<SystemTask> {
    return this.handleRequest(
      this.http.get<SystemTask>(`${this.baseUrl}/system-tasks/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.SYSTEM_TASK_DETAIL,
      `/system-tasks/${namespace}/${name}`,
      'GET'
    );
  }

  updateSystemTask(namespace: string, name: string, taskRequest: UpdateSystemTaskRequest): Observable<SystemTask> {
    return this.handleRequest(
      this.http.put<SystemTask>(`${this.baseUrl}/system-tasks/${namespace}/${name}`, taskRequest, {
        headers: this.getHeaders()
      }),
      LoadingKeys.SYSTEM_TASK_UPDATE,
      `/system-tasks/${namespace}/${name}`,
      'PUT'
    );
  }

  deleteSystemTask(namespace: string, name: string): Observable<void> {
    return this.handleRequest(
      this.http.delete<void>(`${this.baseUrl}/system-tasks/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.SYSTEM_TASK_DELETE,
      `/system-tasks/${namespace}/${name}`,
      'DELETE'
    );
  }

  // LogCollectors
  getLogCollectors(namespace: string = 'polardbx-logcollector'): Observable<PolarDBXLogCollector[]> {
    const params = new HttpParams().set('namespace', namespace);
    return this.handleRequest(
      this.http.get<PolarDBXLogCollector[]>(`${this.baseUrl}/log-collectors`, {
        headers: this.getHeaders(),
        params
      }),
      LoadingKeys.LOG_COLLECTOR_LIST,
      `/log-collectors`,
      'GET'
    );
  }

  createLogCollector(namespace: string, collectorRequest: CreateLogCollectorRequest): Observable<PolarDBXLogCollector> {
    const params = new HttpParams().set('namespace', namespace || collectorRequest.namespace || 'default');
    const body: PolarDBXLogCollector = {
      apiVersion: 'polardbx.aliyun.com/v1',
      kind: 'PolarDBXLogCollector',
      metadata: {
        name: collectorRequest.name,
        namespace: collectorRequest.namespace || namespace || 'default'
      },
      spec: {
        fileBeatName: collectorRequest.fileBeatName,
        logStashName: collectorRequest.logStashName
      }
    } as any;
    return this.handleRequest(
      this.http.post<PolarDBXLogCollector>(`${this.baseUrl}/log-collectors`, body, {
        headers: this.getHeaders(),
        params
      }),
      LoadingKeys.LOG_COLLECTOR_CREATE,
      `/log-collectors`,
      'POST'
    );
  }

  getLogCollector(namespace: string, name: string): Observable<PolarDBXLogCollector> {
    return this.handleRequest(
      this.http.get<PolarDBXLogCollector>(`${this.baseUrl}/log-collectors/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.LOG_COLLECTOR_DETAIL,
      `/log-collectors/${namespace}/${name}`,
      'GET'
    );
  }

  updateLogCollector(namespace: string, collector: PolarDBXLogCollector): Observable<PolarDBXLogCollector> {
    return this.handleRequest(
      this.http.put<PolarDBXLogCollector>(`${this.baseUrl}/log-collectors/${namespace}/${collector.metadata.name}`, collector, {
        headers: this.getHeaders()
      }),
      LoadingKeys.LOG_COLLECTOR_UPDATE,
      `/log-collectors/${namespace}/${collector.metadata.name}`,
      'PUT'
    );
  }

  deleteLogCollector(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/log-collectors/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.LOG_COLLECTOR_DELETE,
      `/log-collectors/${namespace}/${name}`,
      'DELETE'
    );
  }

  // List HPFS sinks (for dropdown selection)
  getHpfsSinks(): Observable<{ namespace: string; configMap: string; sinks: Array<{ name: string; type: string; endpoint?: string; bucket?: string; bucketLookupType?: string; host?: string; port?: number; rootPath?: string; }>}> {
    return this.handleRequest(
      this.http.get<{ namespace: string; configMap: string; sinks: Array<{ name: string; type: string; endpoint?: string; bucket?: string; bucketLookupType?: string; host?: string; port?: number; rootPath?: string; }> }>(`${this.baseUrl}/hpfs/sinks`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_CREATE,
      `/hpfs/sinks`,
      'GET'
    );
  }

  // ============================================================================
  // 🚨 CRITICAL MISSING FUNCTIONALITY: Recovery API Methods
  // Based on document analysis, these are the most critical missing APIs
  // PolarDB-X Operator has complete recovery capabilities but Management Platform had ZERO recovery support
  // ============================================================================

  // Cluster restore operations
  restoreCluster(namespace: string, name: string, request: RestoreClusterRequest): Observable<RestoreResponse> {
    return this.handleRequest(
      this.http.post<RestoreResponse>(`${this.baseUrl}/clusters/${namespace}/${name}/restore`, request, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_RESTORE,
      `/clusters/${namespace}/${name}/restore`,
      'POST'
    );
  }

  initiatePITR(namespace: string, name: string, request: PITRRequest): Observable<PITRResponse> {
    return this.handleRequest(
      this.http.post<PITRResponse>(`${this.baseUrl}/clusters/${namespace}/${name}/pitr`, request, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_PITR,
      `/clusters/${namespace}/${name}/pitr`,
      'POST'
    );
  }

  getRestoreStatus(namespace: string, name: string): Observable<RestoreStatusResponse> {
    return this.handleRequest(
      this.http.get<RestoreStatusResponse>(`${this.baseUrl}/clusters/${namespace}/${name}/restore-status`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.RESTORE_STATUS,
      `/clusters/${namespace}/${name}/restore-status`,
      'GET'
    );
  }

  // Restore job management
  listRestoreJobs(namespace?: string): Observable<RestoreJob[]> {
    const url = `${this.baseUrl}/restore-jobs`;
    const opt: { headers: HttpHeaders; params?: HttpParams } = { headers: this.getHeaders() } as any;
    if (namespace) {
      opt.params = new HttpParams().set('namespace', namespace);
    }
    return this.handleRequest(
      this.http.get<any>(url, opt),
      LoadingKeys.RESTORE_JOB_LIST,
      '/restore-jobs',
      'GET'
    ).pipe(
      map((res: any) => {
        const items = Array.isArray(res) ? res : (res?.items ?? res?.restoreJobs ?? []);
        return (items as any[]).map((it: any) => ({
          clusterName: it.clusterName || it.name,
          namespace: it.namespace,
          phase: it.phase,
          stage: it.stage,
          restoreSpec: it.restoreSpec,
          pitrStatus: it.pitrStatus,
          observedGeneration: it.observedGeneration,
          conditions: it.conditions
        } as RestoreJob));
      })
    );
  }

  getRestoreJob(namespace: string, name: string): Observable<RestoreJob> {
    return this.handleRequest(
      this.http.get<RestoreJob>(`${this.baseUrl}/restore-jobs/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.RESTORE_JOB_DETAIL,
      `/restore-jobs/${namespace}/${name}`,
      'GET'
    );
  }

  cancelRestoreJob(namespace: string, name: string): Observable<CancelRestoreResponse> {
    return this.handleRequest(
      this.http.delete<CancelRestoreResponse>(`${this.baseUrl}/restore-jobs/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.RESTORE_JOB_CANCEL,
      `/restore-jobs/${namespace}/${name}`,
      'DELETE'
    );
  }

  // ============================================================================
  // 🚨 HIGH PRIORITY MISSING FUNCTIONALITY: XStoreFollower API Methods
  // XStoreFollower for DN replica fault recovery (follower rebuild) - identified as critical missing feature
  // ============================================================================

  getXStoreFollowers(namespace?: string): Observable<XStoreFollower[]> {
    const url = `${this.baseUrl}/xstore-followers`;
    let params = this.withNs();
    if (namespace !== undefined) {
      const ns = (namespace || '').trim();
      params = ns ? params.set('namespace', ns) : params.delete('namespace');
    }
    return this.handleRequest(
      this.http.get<XStoreFollower[]>(url, { headers: this.getHeaders(), params }),
      LoadingKeys.XSTORE_LIST,
      '/xstore-followers',
      'GET'
    );
  }

  createXStoreFollower(namespace: string, followerRequest: CreateXStoreFollowerRequest): Observable<XStoreFollower> {
    const ns = (namespace || '').trim();
    const name = (followerRequest?.name || '').trim();
    const xStoreName = (followerRequest?.xStoreName || '').trim();

    const body: any = {
      apiVersion: 'polardbx.aliyun.com/v1',
      kind: 'XStoreFollower',
      metadata: {
        name,
        namespace: ns || undefined
      },
      spec: {
        xStoreName,
        role: followerRequest?.role,
        local: followerRequest?.local,
        nodeName: followerRequest?.nodeName,
        targetPodName: followerRequest?.targetPodName,
        fromPodName: followerRequest?.fromPodName
      }
    };
    const cleaned = JSON.parse(JSON.stringify(body));

    let params = this.withNs();
    if (ns) {
      params = params.set('namespace', ns);
    }
    return this.handleRequest(
      this.http.post<XStoreFollower>(`${this.baseUrl}/xstore-followers`, cleaned, { headers: this.getHeaders(), params }),
      LoadingKeys.XSTORE_CREATE,
      `/xstore-followers`,
      'POST'
    );
  }

  getXStoreFollower(namespace: string, name: string): Observable<XStoreFollower> {
    return this.handleRequest(
      this.http.get<XStoreFollower>(`${this.baseUrl}/xstore-followers/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_FOLLOWER_DETAIL,
      `/xstore-followers/${namespace}/${name}`,
      'GET'
    );
  }

  updateXStoreFollower(namespace: string, follower: XStoreFollower): Observable<XStoreFollower> {
    return this.handleRequest(
      this.http.put<XStoreFollower>(`${this.baseUrl}/xstore-followers/${namespace}/${follower.metadata.name}`, follower, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_FOLLOWER_UPDATE,
      `/xstore-followers/${namespace}/${follower.metadata.name}`,
      'PUT'
    );
  }

  deleteXStoreFollower(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/xstore-followers/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_FOLLOWER_DELETE,
      `/xstore-followers/${namespace}/${name}`,
      'DELETE'
    );
  }

  retryXStoreFollower(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.post(`${this.baseUrl}/xstores/followers/${namespace}/${name}/retry`, {}, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_FOLLOWER_UPDATE,
      `/xstores/followers/${namespace}/${name}/retry`,
      'POST'
    );
  }

  cancelXStoreFollower(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/xstores/followers/${namespace}/${name}/cancel`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_FOLLOWER_DELETE,
      `/xstores/followers/${namespace}/${name}/cancel`,
      'DELETE'
    );
  }

  // ----- Rebuild progress/status helpers -----
  getRebuildProgress(namespace: string, xstoreName: string, followerName: string): Observable<{ name: string; phase?: string; message?: string; targetPod?: string } & any> {
    const url = `${this.baseUrl}/xstores/${encodeURIComponent(namespace)}/${encodeURIComponent(xstoreName)}/rebuild/progress?follower=${encodeURIComponent(followerName)}`;
    return this.handleRequest(
      this.http.get<{ name: string; phase?: string; message?: string; targetPod?: string } & any>(url, { headers: this.getHeaders() }),
      LoadingKeys.XSTORE_LIST,
      `/xstores/${namespace}/${xstoreName}/rebuild/progress`,
      'GET'
    );
  }

  getRebuildStatus(namespace: string, xstoreName: string): Observable<{ namespace: string; xstore: string; active: Array<{ name: string; phase?: string; message?: string; targetPod?: string }> }> {
    const url = `${this.baseUrl}/xstores/${encodeURIComponent(namespace)}/${encodeURIComponent(xstoreName)}/rebuild/status`;
    return this.handleRequest(
      this.http.get<{ namespace: string; xstore: string; active: Array<{ name: string; phase?: string; message?: string; targetPod?: string }> }>(url, { headers: this.getHeaders() }),
      LoadingKeys.XSTORE_LIST,
      `/xstores/${namespace}/${xstoreName}/rebuild/status`,
      'GET'
    );
  }

  // ---- Rebuild wrappers (align with docs: follower / logger / learner / auto) ----
  rebuildFollower(namespace: string, request: CreateXStoreFollowerRequest): Observable<XStoreFollower> {
    return this.createXStoreFollower(namespace, request);
  }

  rebuildLogger(namespace: string, request: CreateXStoreFollowerRequest): Observable<XStoreFollower> {
    // TODO: when backend provides dedicated endpoint, switch here
    return this.createXStoreFollower(namespace, request);
  }

  rebuildLearner(namespace: string, request: CreateXStoreFollowerRequest): Observable<XStoreFollower> {
    // TODO: when backend provides dedicated endpoint, switch here
    return this.createXStoreFollower(namespace, request);
  }

  autoRebuild(namespace: string, request: { xStoreName: string; name?: string; strategy?: string }): Observable<XStoreFollower> {
    // TODO: when backend provides auto strategy endpoint, switch here
    const body: any = { name: request.name, xStoreName: request.xStoreName };
    return this.createXStoreFollower(namespace, body);
  }

  // ==========================================================================
  // XStoreBackup API Methods - Complete the unified backup module
  // ==========================================================================

  listXStoreBackups(namespace: string, view: 'summary'|'detail' = 'detail'): Observable<XStoreBackup[]> {
    const url = `${this.baseUrl}/xstore-backups`;
    const params = this.withNs(new HttpParams().set('view', view));
    return this.handleRequest(
      this.http.get<XStoreBackup[]>(url, { headers: this.getHeaders(), params }),
      LoadingKeys.XSTORE_BACKUP_LIST,
      `/xstore-backups`,
      'GET'
    );
  }

  createXStoreBackup(namespace: string, backupRequest: CreateXStoreBackupRequest): Observable<XStoreBackup> {
    return this.handleRequest(
      this.http.post<XStoreBackup>(`${this.baseUrl}/xstore-backups`, backupRequest, {
        headers: this.getHeaders(),
        params: this.withNs()
      }),
      LoadingKeys.BACKUPS_LIST,
      `/xstore-backups`,
      'POST'
    );
  }

  getXStoreBackup(namespace: string, name: string): Observable<XStoreBackup> {
    return this.handleRequest(
      this.http.get<XStoreBackup>(`${this.baseUrl}/xstore-backups/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_BACKUP_DETAIL,
      `/xstore-backups/${namespace}/${name}`,
      'GET'
    );
  }

  updateXStoreBackup(namespace: string, backup: XStoreBackup): Observable<XStoreBackup> {
    return this.handleRequest(
      this.http.put<XStoreBackup>(`${this.baseUrl}/xstore-backups/${namespace}/${backup.metadata.name}`, backup, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_BACKUP_UPDATE,
      `/xstore-backups/${namespace}/${backup.metadata.name}`,
      'PUT'
    );
  }

  deleteXStoreBackup(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/xstore-backups/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_BACKUP_DELETE,
      `/xstore-backups/${namespace}/${name}`,
      'DELETE'
    );
  }

  // Force delete: remove finalizers
  forceDeleteXStoreBackup(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.post(`${this.baseUrl}/xstore-backups/${namespace}/${name}/force-delete`, {}, {
        headers: this.getHeaders()
      }),
      LoadingKeys.XSTORE_BACKUP_DELETE,
      `/xstore-backups/${namespace}/${name}/force-delete`,
      'POST'
    );
  }

  // ============================================================================
  // BackupBinlog API Methods - Binlog backup & PITR configuration
  // ============================================================================

  getBackupBinlogs(namespace = 'default'): Observable<PolarDBXBackupBinlog[]> {
    const url = `${this.baseUrl}/backup-binlogs`;
    let params = this.withNs();
    if (namespace !== undefined) {
      const ns = (namespace || '').trim();
      params = ns ? params.set('namespace', ns) : params.delete('namespace');
    }
    return this.handleRequest(
      this.http.get<PolarDBXBackupBinlog[]>(url, { headers: this.getHeaders(), params }),
      LoadingKeys.BACKUP_BINLOG_LIST,
      `/backup-binlogs`,
      'GET'
    );
  }

  createBackupBinlog(namespace: string, request: CreateBackupBinlogRequest): Observable<PolarDBXBackupBinlog> {
    const ns = (namespace || '').trim();
    const name = (request?.name || '').trim();
    const pxcName = (request?.pxcName || '').trim();
    const sink = (request?.storageProvider?.sink || '').trim();
    const storageName = (request?.storageProvider?.storageName || '').toString().trim() as any;

    const body: any = {
      apiVersion: 'polardbx.aliyun.com/v1',
      kind: 'PolarDBXBackupBinlog',
      metadata: {
        name,
        namespace: ns || (request?.namespace || '').trim() || undefined
      },
      spec: {
        pxcName,
        pxcUid: request?.pxcUid,
        remoteExpireLogHours: request?.remoteExpireLogHours,
        localExpireLogHours: request?.localExpireLogHours,
        maxLocalBinlogCount: request?.maxLocalBinlogCount,
        pointInTimeRecover: request?.pointInTimeRecover,
        binlogChecksum: request?.binlogChecksum,
        storageProvider: {
          storageName: storageName || undefined,
          sink: sink || undefined
        }
      }
    };
    const cleaned = JSON.parse(JSON.stringify(body));
    let params = this.withNs();
    if (ns) {
      params = params.set('namespace', ns);
    }
    return this.handleRequest(
      this.http.post<PolarDBXBackupBinlog>(`${this.baseUrl}/backup-binlogs`, cleaned, { headers: this.getHeaders(), params }),
      LoadingKeys.BACKUP_BINLOG_CREATE,
      `/backup-binlogs`,
      'POST'
    );
  }

  deleteBackupBinlog(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/backup-binlogs/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_BINLOG_DELETE,
      `/backup-binlogs/${namespace}/${name}`,
      'DELETE'
    );
  }

  getBackupBinlog(namespace: string, name: string): Observable<PolarDBXBackupBinlog> {
    return this.handleRequest(
      this.http.get<PolarDBXBackupBinlog>(`${this.baseUrl}/backup-binlogs/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_BINLOG_DETAIL,
      `/backup-binlogs/${namespace}/${name}`,
      'GET'
    );
  }

  updateBackupBinlog(namespace: string, name: string, request: UpdateBackupBinlogRequest): Observable<PolarDBXBackupBinlog> {
    return this.handleRequest(
      this.http.put<PolarDBXBackupBinlog>(`${this.baseUrl}/backup-binlogs/${namespace}/${name}`, request, {
        headers: this.getHeaders()
      }),
      LoadingKeys.BACKUP_BINLOG_UPDATE,
      `/backup-binlogs/${namespace}/${name}`,
      'PUT'
    );
  }

  // ============================================================================
  // XStoreBackupBinlog API Methods - Standard edition incremental binlog backup
  // ============================================================================

  listXStoreBackupBinlogs(namespace?: string): Observable<any[]> {
    const url = `${this.baseUrl}/xstores/backup-binlogs`;
    const params = namespace ? new HttpParams().set('namespace', namespace) : this.withNs();
    return this.handleRequest(
      this.http.get<any[]>(url, { headers: this.getHeaders(), params }),
      LoadingKeys.BACKUP_BINLOG_LIST,
      `/xstores/backup-binlogs`,
      'GET'
    );
  }

  createXStoreBackupBinlog(namespace: string, body: any): Observable<any> {
    const url = `${this.baseUrl}/xstores/backup-binlogs`;
    return this.handleRequest(
      this.http.post<any>(url, body, { headers: this.getHeaders(), params: this.withNs(new HttpParams().set('namespace', namespace)) }),
      LoadingKeys.BACKUP_BINLOG_CREATE,
      `/xstores/backup-binlogs`,
      'POST'
    );
  }

  getXStoreBackupBinlog(namespace: string, name: string): Observable<any> {
    const url = `${this.baseUrl}/xstores/backup-binlogs/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`;
    return this.handleRequest(
      this.http.get<any>(url, { headers: this.getHeaders() }),
      LoadingKeys.BACKUP_BINLOG_DETAIL,
      `/xstores/backup-binlogs/${namespace}/${name}`,
      'GET'
    );
  }

  updateXStoreBackupBinlog(namespace: string, body: any): Observable<any> {
    const url = `${this.baseUrl}/xstores/backup-binlogs/${encodeURIComponent(namespace)}/${encodeURIComponent(body?.metadata?.name || '')}`;
    return this.handleRequest(
      this.http.put<any>(url, body, { headers: this.getHeaders() }),
      LoadingKeys.BACKUP_BINLOG_UPDATE,
      `/xstores/backup-binlogs/${namespace}/${body?.metadata?.name || ''}`,
      'PUT'
    );
  }

  deleteXStoreBackupBinlog(namespace: string, name: string): Observable<any> {
    const url = `${this.baseUrl}/xstores/backup-binlogs/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`;
    return this.handleRequest(
      this.http.delete<any>(url, { headers: this.getHeaders() }),
      LoadingKeys.BACKUP_BINLOG_DELETE,
      `/xstores/backup-binlogs/${namespace}/${name}`,
      'DELETE'
    );
  }

  // PolarDBXClusterKnobs management API - performance tuning parameter management
  getClusterKnobsList(): Observable<PolarDBXClusterKnobsList> {
    return this.handleRequest(
      this.http.get<PolarDBXClusterKnobsList>(`${this.baseUrl}/cluster-knobs`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_KNOBS_LIST,
      '/cluster-knobs',
      'GET'
    );
  }

  createClusterKnobs(namespace: string, request: CreateClusterKnobsRequest): Observable<PolarDBXClusterKnobs> {
    return this.handleRequest(
      this.http.post<PolarDBXClusterKnobs>(`${this.baseUrl}/cluster-knobs`, request, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_KNOBS_CREATE,
      '/cluster-knobs',
      'POST'
    );
  }

  getClusterKnobs(namespace: string, name: string): Observable<PolarDBXClusterKnobs> {
    return this.handleRequest(
      this.http.get<PolarDBXClusterKnobs>(`${this.baseUrl}/cluster-knobs/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_KNOBS_DETAIL,
      `/cluster-knobs/${namespace}/${name}`,
      'GET'
    );
  }

  updateClusterKnobs(namespace: string, knobs: PolarDBXClusterKnobs): Observable<PolarDBXClusterKnobs> {
    return this.handleRequest(
      this.http.put<PolarDBXClusterKnobs>(`${this.baseUrl}/cluster-knobs/${namespace}/${knobs.metadata.name}`, knobs, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_KNOBS_UPDATE,
      `/cluster-knobs/${namespace}/${knobs.metadata.name}`,
      'PUT'
    );
  }

  deleteClusterKnobs(namespace: string, name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/cluster-knobs/${namespace}/${name}`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.CLUSTER_KNOBS_DELETE,
      `/cluster-knobs/${namespace}/${name}`,
      'DELETE'
    );
  }

  // Get backup overview (supports optional connectivity check/storage estimation parameters)
  getBackupOverview(params?: { namespace?: string; evaluateConnectivity?: boolean; evaluateStorage?: boolean; systemNamespace?: string }): Observable<any> {
    const url = `${this.baseUrl}/backups/overview`;
    let httpParams = this.withNs();

    if (params && 'namespace' in params) {
      const ns = (params.namespace || '').trim();
      httpParams = ns ? httpParams.set('namespace', ns) : httpParams.delete('namespace');
    }
    if (params?.evaluateConnectivity) {
      httpParams = httpParams.set('evaluateConnectivity', 'true');
    }
    if (params?.evaluateStorage) {
      httpParams = httpParams.set('evaluateStorage', 'true');
    }
    if (params?.systemNamespace) {
      httpParams = httpParams.set('systemNamespace', params.systemNamespace);
    }

    return this.handleRequest(
      this.http.get<any>(url, { headers: this.getHeaders(), params: httpParams }),
      LoadingKeys.BACKUPS_LIST,
      '/backups/overview',
      'GET'
    );
  }

  // Get backup status for each cluster (latest backup, next schedule, RPO)
  getClusterBackupState(namespace?: string): Observable<{ namespace: string; total: number; clusters: any[] }> {
    const url = `${this.baseUrl}/backups/cluster-state`;
    let params = this.withNs();
    if (namespace !== undefined) {
      const ns = (namespace || '').trim();
      params = ns ? params.set('namespace', ns) : params.delete('namespace');
    }
    return this.handleRequest(
      this.http.get<{ namespace: string; total: number; clusters: any[] }>(url, { headers: this.getHeaders(), params }),
      LoadingKeys.BACKUPS_LIST,
      '/backups/cluster-state',
      'GET'
    );
  }

  // Get Binlog aggregated metrics.
  // Note: `throughputMBps` may be omitted when backend cannot provide real throughput (we do NOT use placeholders).
  // `estimateThroughput` is kept for forward compatibility but may have no effect depending on backend capability.
  getBinlogMetrics(namespace: string, estimateThroughput = false, windowSeconds = 300): Observable<any> {
    const url = `${this.baseUrl}/backups/binlog/metrics`;
    const params = this.withNs(new HttpParams()
      .set('estimateThroughput', estimateThroughput ? 'true' : 'false')
      .set('throughputWindowSeconds', String(windowSeconds)));
    return this.handleRequest(
      this.http.get<any>(url, { headers: this.getHeaders(), params }),
      LoadingKeys.BACKUP_BINLOG_LIST,
      '/backups/binlog/metrics',
      'GET'
    ).pipe(map(res => res?.binlogs ?? []));
  }

  // Read/update backup dashboard threshold settings
  getBackupDashboardSettings(): Observable<{ rpoThresholdSeconds: number; throughputLowerBoundMBps: number; diagnosisRetentionDays: number }> {
    return this.handleRequest(
      this.http.get<{ rpoThresholdSeconds: number; throughputLowerBoundMBps: number; diagnosisRetentionDays: number }>(`${this.baseUrl}/settings/backup-dashboard`, { headers: this.getHeaders() }),
      LoadingKeys.PARAMETER_TEMPLATE_LIST,
      '/settings/backup-dashboard',
      'GET'
    );
  }

  updateBackupDashboardSettings(req: { rpoThresholdSeconds: number; throughputLowerBoundMBps: number; diagnosisRetentionDays: number }): Observable<any> {
    return this.handleRequest(
      this.http.put(`${this.baseUrl}/settings/backup-dashboard`, req, { headers: this.getHeaders() }),
      LoadingKeys.PARAMETER_TEMPLATE_UPDATE,
      '/settings/backup-dashboard',
      'PUT'
    );
  }

  // Pre-change validation (unified interface)
  runPrecheck(namespace: string, name: string, operation: 'scale'|'upgrade'|'config', targetSpec?: any): Observable<any> {
    const url = `${this.baseUrl}/clusters/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/precheck`;
    return this.handleRequest(
      this.http.post<any>(url, { operation, targetSpec: targetSpec || {} }, { headers: this.getHeaders() }),
      LoadingKeys.PRECHECK_RUN,
      `/clusters/${namespace}/${name}/precheck`,
      'POST'
    );
  }

  // Alert aggregation
  listAlerts(params?: { namespace?: string; cluster?: string; alertmanager?: string }): Observable<{ total: number; items: any[] }> {
    const q: string[] = [];
    if (params?.namespace) q.push(`namespace=${encodeURIComponent(params.namespace)}`);
    if (params?.cluster) q.push(`cluster=${encodeURIComponent(params.cluster)}`);
    if (params?.alertmanager) q.push(`alertmanager=${encodeURIComponent(params.alertmanager)}`);
    const url = `${this.baseUrl}/alerts${q.length ? ('?' + q.join('&')) : ''}`;
    return this.handleRequest(
      this.http.get<{ total: number; items: any[] }>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
      '/alerts',
      'GET'
    );
  }

  // Diagnostics: start, status, list reports, download
  startDiagnosis(namespace: string, cluster: string): Observable<any> {
    const url = `${this.baseUrl}/diagnostics/${encodeURIComponent(namespace)}/${encodeURIComponent(cluster)}/start`;
    return this.handleRequest(
      this.http.post(url, {}, { headers: this.getHeaders() }),
      LoadingKeys.CLUSTER_DETAIL,
      `/diagnostics/${namespace}/${cluster}/start`,
      'POST'
    );
  }

  getDiagnosisStatus(namespace: string, id: string): Observable<any> {
    const url = `${this.baseUrl}/diagnostics/${encodeURIComponent(namespace)}/${encodeURIComponent(id)}/status`;
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders() }),
      LoadingKeys.CLUSTER_DETAIL,
      `/diagnostics/${namespace}/${id}/status`,
      'GET'
    );
  }

  listDiagnosisReports(namespace?: string): Observable<any[]> {
    const url = `${this.baseUrl}/diagnostics/reports`;
    const params = (namespace && namespace.trim())
      ? new HttpParams().set('namespace', namespace.trim())
      : this.withNs();
    return this.handleRequest(
      this.http.get<{ reports: any[] }>(url, { headers: this.getHeaders(), params }),
      LoadingKeys.CLUSTER_DETAIL,
      '/diagnostics/reports',
      'GET'
    ).pipe(map(res => res?.reports ?? []));
  }

  downloadDiagnosisReport(namespace: string, id: string): Observable<{ url: string }> {
    const url = `${this.baseUrl}/diagnostics/${encodeURIComponent(namespace)}/${encodeURIComponent(id)}/download`;
    return this.handleRequest(
      this.http.get<{ url: string }>(url, { headers: this.getHeaders() }),
      LoadingKeys.CLUSTER_DETAIL,
      `/diagnostics/${namespace}/${id}/download`,
      'GET'
    );
  }

  deleteDiagnosisReport(namespace: string, id: string): Observable<any> {
    const url = `${this.baseUrl}/diagnostics/${encodeURIComponent(namespace)}/${encodeURIComponent(id)}`;
    return this.handleRequest(
      this.http.delete(url, { headers: this.getHeaders() }),
      LoadingKeys.CLUSTER_DETAIL,
      `/diagnostics/${namespace}/${id}`,
      'DELETE'
    );
  }

  downloadDiagnosisReportFile(namespace: string, id: string): Observable<Blob> {
    const url = `${this.baseUrl}/diagnostics/${encodeURIComponent(namespace)}/${encodeURIComponent(id)}/file`;
    // Note: use HttpClient directly to keep responseType as blob.
    return this.http.get(url, { headers: this.getHeaders(), responseType: 'blob' });
  }

  // Pre-change: create SystemTask for precheck
  createPrecheckSystemTask(namespace: string, name: string): Observable<any> {
    const url = `${this.baseUrl}/clusters/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}/precheck-systemtask`;
    return this.handleRequest(
      this.http.post(url, {}, { headers: this.getHeaders() }),
      LoadingKeys.CLUSTER_DETAIL,
      `/clusters/${namespace}/${name}/precheck-systemtask`,
      'POST'
    );
  }

  // -----------------------------
  // System meta
  // -----------------------------
  getSystemContext(): Observable<{ user?: string; context?: string; defaultNamespace?: string }> {
    const url = `${this.baseUrl}/system/context`;
    return this.handleRequest(
      this.http.get<{ user?: string; context?: string; defaultNamespace?: string }>(url, { headers: this.getHeaders() }),
      LoadingKeys.CONNECT,
      '/system/context',
      'GET'
    );
  }

  listSystemNamespaces(opts?: { silent?: boolean }): Observable<{ items: { name: string; status: string; createdAt: string }[]; count: number }> {
    const url = `${this.baseUrl}/system/namespaces`;
    return this.handleRequest(
      this.http.get<{ items: { name: string; status: string; createdAt: string }[]; count: number }>(url, { headers: this.getHeaders() }),
      LoadingKeys.SYSTEM,
      '/system/namespaces',
      'GET',
      opts
    );
  }

  // -----------------------------
  // Monitoring stack
  // -----------------------------
  getMonitoringStatus(namespace?: string): Observable<any> {
    const url = `${this.baseUrl}/monitoring/status`;
    const params = namespace ? new HttpParams().set('namespace', namespace) : new HttpParams();
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders(), params }),
      LoadingKeys.MONITOR_LIST,
      '/monitoring/status',
      'GET'
    );
  }

  monitoringBootstrap(req: { mode: 'managed'|'assisted'|'byo'; namespace?: string; releaseName?: string; dryRun?: boolean }): Observable<any> {
    const url = `${this.baseUrl}/monitoring/bootstrap`;
    return this.handleRequest(
      this.http.post(url, req || {}, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_CREATE,
      '/monitoring/bootstrap',
      'POST'
    );
  }

  monitoringBootstrapStatus(jobName: string, namespace?: string): Observable<any> {
    const url = `${this.baseUrl}/monitoring/bootstrap/status`;
    const params = new HttpParams()
      .set('jobName', jobName)
      .set('namespace', namespace || 'polardbx-operator-system');
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders(), params }),
      LoadingKeys.MONITOR_LIST,
      '/monitoring/bootstrap/status',
      'GET'
    );
  }

  monitoringBootstrapLogs(jobName: string, namespace?: string, tailLines?: number): Observable<any> {
    const url = `${this.baseUrl}/monitoring/bootstrap/logs`;
    let params = new HttpParams()
      .set('jobName', jobName)
      .set('namespace', namespace || 'polardbx-operator-system');
    if (tailLines) {
      params = params.set('tailLines', tailLines.toString());
    }
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders(), params }),
      LoadingKeys.MONITOR_LIST,
      '/monitoring/bootstrap/logs',
      'GET'
    );
  }

  // Log collection related interfaces
  logsBootstrap(req: { mode: 'managed'|'assisted'|'byo'; namespace?: string; releaseName?: string; dryRun?: boolean; enableFilebeat?: boolean; enableLogstash?: boolean; esHost?: string; esUser?: string; esPassword?: string; esIndex?: string; deploymentType?: string }): Observable<any> {
    const url = `${this.baseUrl}/logs/bootstrap`;
    return this.handleRequest(
      this.http.post(url, req || {}, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_CREATE,
      '/logs/bootstrap',
      'POST'
    );
  }

  logsBootstrapStatus(jobName: string, namespace?: string): Observable<any> {
    const url = `${this.baseUrl}/logs/bootstrap/status`;
    const params = new HttpParams()
      .set('jobName', jobName)
      .set('namespace', namespace || 'polardbx-logcollector');
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders(), params }),
      LoadingKeys.MONITOR_LIST,
      '/logs/bootstrap/status',
      'GET',
      { silent: true }
    );
  }

  logsBootstrapLogs(jobName: string, namespace?: string, tailLines?: number): Observable<any> {
    const url = `${this.baseUrl}/logs/bootstrap/logs`;
    let params = new HttpParams()
      .set('jobName', jobName)
      .set('namespace', namespace || 'polardbx-logcollector');
    if (tailLines) {
      params = params.set('tailLines', tailLines.toString());
    }
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders(), params }),
      LoadingKeys.MONITOR_LIST,
      '/logs/bootstrap/logs',
      'GET',
      { silent: true }
    );
  }

  monitoringUninstall(namespace?: string): Observable<any> {
    const url = `${this.baseUrl}/monitoring/uninstall`;
    return this.handleRequest(
      this.http.delete(url, { headers: this.getHeaders(), params: this.withNs() }),
      LoadingKeys.MONITOR_UPDATE,
      '/monitoring/uninstall',
      'DELETE'
    );
  }

  // Alias for consistency
  uninstallMonitoring(namespace?: string): Observable<any> {
    return this.monitoringUninstall(namespace);
  }

  // -----------------------------
  // Grafana
  // -----------------------------
  getGrafanaConfig(): Observable<any> {
  const url = `${this.baseUrl}/platform/grafana/config`;
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
  '/platform/grafana/config',
      'GET'
    );
  }

  putGrafanaConfig(cfg: any): Observable<any> {
  const url = `${this.baseUrl}/platform/grafana/config`;
    return this.handleRequest(
      this.http.put(url, cfg, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
  '/platform/grafana/config',
      'PUT'
    );
  }

  // -----------------------------
  // System settings (generic key-value in ConfigMap)
  // -----------------------------
  getSystemSettings(): Observable<Record<string, string>> {
    const url = `${this.baseUrl}/settings`;
    return this.handleRequest(
      this.http.get<Record<string, string>>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
      '/settings',
      'GET'
    );
  }

  updateSystemSettings(body: Record<string, any>): Observable<any> {
    const url = `${this.baseUrl}/settings`;
    return this.handleRequest(
      this.http.put(url, body || {}, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
      '/settings',
      'PUT'
    );
  }

  syncGrafanaDashboards(payload: { dashboards: Record<string, string>; overwrite?: boolean }): Observable<any> {
  const url = `${this.baseUrl}/platform/grafana/dashboards/sync`;
    return this.handleRequest(
      this.http.post(url, payload, { headers: this.getHeaders() }),
      LoadingKeys.GRAFANA_TEMPLATE_IMPORT,
  '/platform/grafana/dashboards/sync',
      'POST'
    );
  }

  listGrafanaTemplates(): Observable<{ items: GrafanaTemplateSummary[]; directory: string }> {
  const url = `${this.baseUrl}/platform/grafana/templates`;
    return this.handleRequest(
      this.http.get<{ items: GrafanaTemplateSummary[]; directory: string }>(url, { headers: this.getHeaders() }),
      LoadingKeys.GRAFANA_TEMPLATE_LIST,
  '/platform/grafana/templates',
      'GET'
    );
  }

  getGrafanaTemplate(name: string, opts?: { silent?: boolean }): Observable<GrafanaTemplateDetail> {
  const url = `${this.baseUrl}/platform/grafana/templates/${encodeURIComponent(name)}`;
    return this.handleRequest(
      this.http.get<GrafanaTemplateDetail>(url, { headers: this.getHeaders() }),
      LoadingKeys.GRAFANA_TEMPLATE_DETAIL,
  `/platform/grafana/templates/${name}`,
      'GET',
      opts
    );
  }

  listAlertRuleTemplates(): Observable<{ items: AlertRuleTemplateSummary[]; directory: string }> {
    const url = `${this.baseUrl}/prometheus-rules/templates`;
    return this.handleRequest(
      this.http.get<{ items: AlertRuleTemplateSummary[]; directory: string }>(url, { headers: this.getHeaders() }),
      LoadingKeys.ALERT_TEMPLATE_LIST,
      '/prometheus-rules/templates',
      'GET'
    );
  }

  getAlertRuleTemplate(name: string): Observable<AlertRuleTemplateDetail> {
    const url = `${this.baseUrl}/prometheus-rules/templates/${encodeURIComponent(name)}`;
    return this.handleRequest(
      this.http.get<AlertRuleTemplateDetail>(url, { headers: this.getHeaders() }),
      LoadingKeys.ALERT_TEMPLATE_DETAIL,
      `/prometheus-rules/templates/${name}`,
      'GET'
    );
  }

  applyAlertRuleTemplate(payload: ApplyAlertRuleTemplatePayload): Observable<ApplyAlertRuleTemplateResponse> {
    const url = `${this.baseUrl}/prometheus-rules/apply`;
    return this.handleRequest(
      this.http.post<ApplyAlertRuleTemplateResponse>(url, payload, { headers: this.getHeaders() }),
      LoadingKeys.ALERT_TEMPLATE_APPLY,
      '/prometheus-rules/apply',
      'POST'
    );
  }

  // Grafana dashboards versioning
  listDashboards(): Observable<{ items: { name: string; versions: number }[] }> {
  const url = `${this.baseUrl}/platform/grafana/dashboards`;
    return this.handleRequest(
      this.http.get<{ items: { name: string; versions: number }[] }>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
  '/platform/grafana/dashboards',
      'GET'
    );
  }

  listDashboardVersions(name: string): Observable<{ name: string; versions: number[] }> {
  const url = `${this.baseUrl}/platform/grafana/dashboards/${encodeURIComponent(name)}/versions`;
    return this.handleRequest(
      this.http.get<{ name: string; versions: number[] }>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
  `/platform/grafana/dashboards/${name}/versions`,
      'GET'
    );
  }

  rollbackDashboard(name: string, version: number): Observable<any> {
  const url = `${this.baseUrl}/platform/grafana/dashboards/${encodeURIComponent(name)}/rollback`;
    return this.handleRequest(
      this.http.post(url, { version }, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
  `/platform/grafana/dashboards/${name}/rollback`,
      'POST'
    );
  }

  // -----------------------------
  // Alerts: profiles, routes, silences, test
  // -----------------------------
  listAlertProfiles(): Observable<{ names: string[] }> {
    const url = `${this.baseUrl}/alerts/profiles`;
    return this.handleRequest(
      this.http.get<{ names: string[] }>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
      '/alerts/profiles',
      'GET'
    );
  }

  createAlertProfile(name: string, content: string): Observable<{ name: string }> {
    const url = `${this.baseUrl}/alerts/profiles`;
    return this.handleRequest(
      this.http.post<{ name: string }>(url, { name, content }, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_CREATE,
      '/alerts/profiles',
      'POST'
    );
  }

  getAlertProfile(name: string): Observable<{ name: string; content: string }> {
    const url = `${this.baseUrl}/alerts/profiles/${encodeURIComponent(name)}`;
    return this.handleRequest(
      this.http.get<{ name: string; content: string }>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
      `/alerts/profiles/${name}`,
      'GET'
    );
  }

  updateAlertProfile(name: string, content: string): Observable<{ name: string }> {
    const url = `${this.baseUrl}/alerts/profiles/${encodeURIComponent(name)}`;
    return this.handleRequest(
      this.http.put<{ name: string }>(url, { content }, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
      `/alerts/profiles/${name}`,
      'PUT'
    );
  }

  deleteAlertProfile(name: string): Observable<any> {
    const url = `${this.baseUrl}/alerts/profiles/${encodeURIComponent(name)}`;
    return this.handleRequest(
      this.http.delete(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
      `/alerts/profiles/${name}`,
      'DELETE'
    );
  }

  dryRunAlertProfile(content: string): Observable<{ valid: boolean; error?: string }> {
    const url = `${this.baseUrl}/alerts/profiles/dry-run`;
    return this.handleRequest(
      this.http.post<{ valid: boolean; error?: string }>(url, { content }, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
      '/alerts/profiles/dry-run',
      'POST'
    );
  }

  getAlertRoutes(): Observable<{ content: string; alertmanagerUrl?: string }> {
    const url = `${this.baseUrl}/alerts/routes`;
    return this.handleRequest(
      this.http.get<{ content: string; alertmanagerUrl?: string }>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
      '/alerts/routes',
      'GET'
    );
  }

  putAlertRoutes(payload: { content: string; alertmanagerUrl?: string }): Observable<any> {
    const url = `${this.baseUrl}/alerts/routes`;
    return this.handleRequest(
      this.http.put(url, payload, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
      '/alerts/routes',
      'PUT'
    );
  }

  listSilences(): Observable<any[]> {
    const url = `${this.baseUrl}/alerts/silences`;
    return this.handleRequest(
      this.http.get<any[]>(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_LIST,
      '/alerts/silences',
      'GET'
    );
  }

  createSilence(body: any): Observable<any> {
    const url = `${this.baseUrl}/alerts/silences`;
    return this.handleRequest(
      this.http.post(url, body, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_CREATE,
      '/alerts/silences',
      'POST'
    );
  }

  deleteSilence(id: string): Observable<any> {
    const url = `${this.baseUrl}/alerts/silences/${encodeURIComponent(id)}`;
    return this.handleRequest(
      this.http.delete(url, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_UPDATE,
      `/alerts/silences/${id}`,
      'DELETE'
    );
  }

  testAlert(body: any): Observable<any> {
    const url = `${this.baseUrl}/alerts/test`;
    let params = new HttpParams();

    const payload = (body && typeof body === 'object') ? body as Record<string, unknown> : null;
    const alertmanagerUrl = (payload?.['alertmanagerUrl'] ?? payload?.['alertmanager']) as unknown;
    if (typeof alertmanagerUrl === 'string' && alertmanagerUrl.trim()) {
      params = params.set('alertmanager', alertmanagerUrl.trim());
    }

    const labels = payload?.['labels'] as unknown;
    if (typeof labels === 'string' && labels.trim()) {
      params = params.set('labels', labels.trim());
    } else if (labels && typeof labels === 'object') {
      const entries = Object.entries(labels as Record<string, unknown>)
        .map(([k, v]) => [String(k).trim(), v == null ? '' : String(v).trim()] as const)
        .filter(([k, v]) => !!k && !!v);
      if (entries.length) {
        params = params.set('labels', entries.map(([k, v]) => `${k}=${v}`).join(','));
      }
    }

    return this.handleRequest(
      this.http.post(url, {}, { headers: this.getHeaders(), params }),
      LoadingKeys.MONITOR_CREATE,
      '/alerts/test',
      'POST'
    );
  }

  applyAlertReceiverConfig(payload: any): Observable<any> {
    const url = `${this.baseUrl}/alerts/receivers/apply`;
    return this.handleRequest(
      this.http.post(url, payload || {}, { headers: this.getHeaders() }),
      LoadingKeys.MONITOR_CREATE,
      '/alerts/receivers/apply',
      'POST'
    );
  }

  detectMonitoringEnvironment(namespace?: string): Observable<any> {
    const url = `${this.baseUrl}/monitoring/detect`;
    const storedNs = (localStorage.getItem('activeNamespace') || '').trim();
    const finalNamespace = (namespace || '').trim() || storedNs;
    let params = new HttpParams();
    if (finalNamespace) {
      params = params.set('namespace', finalNamespace);
    }
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders(), params }),
      LoadingKeys.MONITOR_LIST,
      '/monitoring/detect',
      'GET'
    );
  }

  getMonitoringPreflight(namespace?: string): Observable<any> {
    const url = `${this.baseUrl}/monitoring/preflight`;
    return this.handleRequest(
      this.http.get(url, { headers: this.getHeaders(), params: this.withNs() }),
      LoadingKeys.MONITOR_LIST,
      '/monitoring/preflight',
      'GET'
    );
  }

  // -----------------------------
  // Logs
  // -----------------------------
  getLogPresets(): Observable<LogsPresetList> {
    return this.handleRequest(
      this.http.get<LogsPresetList>(`${this.baseUrl}/logs/presets`, { headers: this.getHeaders() }),
      LoadingKeys.LOGS_PRESETS,
      '/logs/presets',
      'GET'
    );
  }

  queryLogs(req: LogsQueryRequest): Observable<any | NormalizedResponse> {
    return this.handleRequest(
      this.http.post(`${this.baseUrl}/logs/query`, req || {}, { headers: this.getHeaders() }),
      LoadingKeys.LOGS_QUERY,
      '/logs/query',
      'POST'
    );
  }

  // Log Service Dashboard Methods
  // -----------------------------
  getLogServiceStatus(namespace?: string): Observable<any> {
    const params = namespace ? new HttpParams().set('namespace', namespace) : new HttpParams();
    return this.handleRequest(
      this.http.get(`${this.baseUrl}/log-service/status`, { headers: this.getHeaders(), params }),
      LoadingKeys.LOG_SERVICE_STATUS,
      '/log-service/status',
      'GET'
    );
  }

  getLogStrategies(): Observable<any[]> {
    const req = this.http
      .get<any>(`${this.baseUrl}/log-strategies`, { headers: this.getHeaders() })
      .pipe(
        catchError((error: HttpErrorResponse) => {
          // Handle HTTP error status codes (500, 401, etc.) properly
          // Backend now returns proper error status codes instead of 200 with warning
          this.errorHandler.handleHttpError(error, '加载日志策略失败');
          return throwError(() => error);
        }),
        map((res: any) => (Array.isArray(res) ? res : (res?.items || []))),
        map((items: any[]) => (items || []).map((s: any) => {
          const output = s?.output || {};
          const type = (output.type || s?.outputType || 'stdout').toLowerCase();
          const hostsStr: string = output.hosts || '';
          const hostsArr = hostsStr ? hostsStr.split(',').map((h: string) => h.trim()).filter(Boolean) : [];
          return {
            name: s?.name,
            targetCluster: s?.clusterName || s?.targetCluster || '',
            outputType: type,
            status: s?.status || 'active',
            config: type === 'elasticsearch' ? {
              elasticsearch: {
                hosts: hostsArr,
                username: output.username || '',
                password: '',
                index: output.index || ''
              }
            } : {},
            createdAt: s?.createdAt || '',
            updatedAt: s?.updatedAt || ''
          };
        }))
      );
    return this.handleRequest<any[]>(
      req,
      LoadingKeys.LOG_STRATEGIES_LIST,
      '/log-strategies',
      'GET'
    );
  }

  createLogStrategy(strategy: any): Observable<any> {
    // Map frontend strategy model to backend required model
    const payload = this.mapToBackendLogStrategy(strategy);
    return this.handleRequest(
      this.http.post(`${this.baseUrl}/log-strategies`, payload, { headers: this.getHeaders() }),
      LoadingKeys.LOG_STRATEGY_SAVE,
      '/log-strategies',
      'POST'
    );
  }

  updateLogStrategy(name: string, strategy: any): Observable<any> {
    const payload = this.mapToBackendLogStrategy({ ...strategy, name });
    return this.handleRequest(
      this.http.put(`${this.baseUrl}/log-strategies/${name}`, payload, { headers: this.getHeaders() }),
      LoadingKeys.LOG_STRATEGY_SAVE,
      `/log-strategies/${name}`,
      'PUT'
    );
  }

  deleteLogStrategy(name: string): Observable<any> {
    return this.handleRequest(
      this.http.delete(`${this.baseUrl}/log-strategies/${name}`, { headers: this.getHeaders() }),
      LoadingKeys.LOG_STRATEGY_DELETE,
      `/log-strategies/${name}`,
      'DELETE'
    );
  }

  testElasticsearchConnection(config: any): Observable<any> {
    return this.handleRequest(
      this.http.post(`${this.baseUrl}/log-strategies/test-connection`, config, { headers: this.getHeaders() }),
      LoadingKeys.ES_CONNECTION_TEST,
      '/log-strategies/test-connection',
      'POST'
    );
  }

  // Convert UI model to backend required Strategy model
  private mapToBackendLogStrategy(ui: any): any {
    const name: string = ui?.name || '';
    const clusterName: string = ui?.targetCluster || ui?.clusterName || '';
    const outputType: string = (ui?.outputType || ui?.output?.type || 'stdout').toLowerCase();
    // Handle ES configuration
    const es = ui?.config?.elasticsearch || {};
    const hostsArr: string[] = Array.isArray(es.hosts) ? es.hosts : (es.hosts ? [es.hosts] : []);
    const hostsJoined = hostsArr.filter(Boolean).join(',');
    const username: string = es.username || ui?.esUsername || '';
    const password: string = es.password || ui?.esPassword || '';
    const useTLS = hostsArr.some((h) => typeof h === 'string' && h.trim().toLowerCase().startsWith('https://'));
    const authType = username ? 'basic' : 'none';

    return {
      name: name,
      clusterName: clusterName,
      // clusterNamespace can be omitted, backend defaults to 'default'
      enableCN: true,
      enableDN: true,
      output: {
        type: outputType,
        hosts: hostsJoined,
        authType: authType,
        username: username,
        password: password,
        useTLS: useTLS,
        caCrt: ''
      }
    };
  }

  // PrometheusRule Management Methods
  getPrometheusRules(namespace?: string): Observable<any[]> {
    const params = namespace ? new HttpParams().set('namespace', namespace) : this.withNs();
    return this.handleRequest(
      this.http.get<any[]>(`${this.baseUrl}/prometheus-rules`, {
        headers: this.getHeaders(),
        params
      }),
      LoadingKeys.MONITORING,
      '/prometheus-rules',
      'GET'
    );
  }

  getPrometheusRuleYaml(namespace: string, name: string): Observable<string> {
    return this.handleRequest(
      this.http.get(`${this.baseUrl}/prometheus-rules/${namespace}/${name}/yaml`, {
        headers: this.getHeaders(),
        responseType: 'text'
      }),
      LoadingKeys.MONITORING,
      `/prometheus-rules/${namespace}/${name}/yaml`,
      'GET'
    );
  }

  validatePrometheusRule(yamlContent: string): Observable<any> {
    return this.handleRequest(
      this.http.post<any>(`${this.baseUrl}/prometheus-rules/validate`, {
        yaml: yamlContent
      }, {
        headers: this.getHeaders()
      }),
      LoadingKeys.MONITORING,
      '/prometheus-rules/validate',
      'POST'
    );
  }

  // Namespace Methods
  getNamespaces(): Observable<string[]> {
    const req = this.http.get<any>(`${this.baseUrl}/namespaces`, { headers: this.getHeaders() })
      .pipe(
        catchError((error: HttpErrorResponse) => {
          // Handle HTTP error status codes (500, 401, etc.) properly
          // Backend now returns proper error status codes instead of 200 with warning
          // Silently handle namespace loading errors to avoid disrupting UI
          console.warn('Failed to load namespaces:', error);
          return of([]); // Return empty array on error
        }),
        map((res: any) => {
          if (Array.isArray(res)) return res as string[];
          if (Array.isArray(res?.items)) {
            // items may be string array or object array
            if (res.items.length > 0 && typeof res.items[0] === 'object') {
              return (res.items as any[]).map((it: any) => it?.name || it?.metadata?.name).filter(Boolean);
            }
            return res.items as string[];
          }
          if (Array.isArray(res?.names)) return res.names as string[];
          return [] as string[];
        })
      );
    return this.handleRequest(
      req,
      LoadingKeys.SYSTEM,
      '/namespaces',
      'GET'
    );
  }

  getLogStrategyApplyRecords(): Observable<any[]> {
    const req = this.http.get<any>(`${this.baseUrl}/log-strategies/apply-records`, { headers: this.getHeaders() })
      .pipe(
        catchError((error: HttpErrorResponse) => {
          // Handle HTTP error status codes (500, 401, etc.) properly
          // Backend now returns proper error status codes instead of 200 with empty list
          // Return empty array on error to avoid disrupting UI, but log the error
          console.warn('Failed to load log strategy apply records:', error);
          return of([]);
        }),
        map((res: any) => Array.isArray(res) ? res : (res?.items || []))
      );
    return this.handleRequest(
      req,
      LoadingKeys.LOG_STRATEGY,
      '/log-strategies/apply-records',
      'GET',
      { silent: true } // Silent error handling since we handle it in the pipe
    );
  }


  precheckLogStrategy(strategy: any): Observable<any> {
    return this.handleRequest(
      this.http.post<any>(`${this.baseUrl}/log-strategies/precheck`, strategy, {
        headers: this.getHeaders()
      }),
      LoadingKeys.LOG_STRATEGY,
      '/log-strategies/precheck',
      'POST'
    );
  }

  applyLogStrategy(id: string): Observable<any> {
    return this.handleRequest(
      this.http.post<any>(`${this.baseUrl}/log-strategies/${id}/apply`, {}, {
        headers: this.getHeaders()
      }),
      LoadingKeys.LOG_STRATEGY,
      `/log-strategies/${id}/apply`,
      'POST'
    );
  }

  // ==================== Image Registry Configuration API ====================

  getImageRegistryPresets(): Observable<any[]> {
    return this.handleRequest(
      this.http.get<any[]>(`${this.baseUrl}/image-registry/presets`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.SYSTEM,
      '/image-registry/presets',
      'GET'
    );
  }

  getImageRegistryConfig(): Observable<any> {
    return this.handleRequest(
      this.http.get<any>(`${this.baseUrl}/image-registry/config`, {
        headers: this.getHeaders()
      }),
      LoadingKeys.SYSTEM,
      '/image-registry/config',
      'GET'
    );
  }

  setImageRegistryConfig(config: any): Observable<any> {
    return this.handleRequest(
      this.http.put<any>(`${this.baseUrl}/image-registry/config`, config, {
        headers: this.getHeaders()
      }),
      LoadingKeys.SYSTEM,
      '/image-registry/config',
      'PUT'
    );
  }

  // ==================== System Configuration API (for cluster creation wizard) ====================

  /**
   * Get list of available StorageClasses in Kubernetes cluster
   */
  getStorageClasses(): Observable<any[]> {
    const req = this.http.get<any>(`${this.baseUrl}/platform/system/storage-classes`, { headers: this.getHeaders() })
      .pipe(
        map((res: any) => {
          if (Array.isArray(res)) return res;
          if (Array.isArray(res?.items)) return res.items;
          return [];
        })
      );
    return this.handleRequest(
      req,
      LoadingKeys.SYSTEM,
      '/platform/system/storage-classes',
      'GET'
    );
  }

  /**
   * Get list of supported PolarDB-X versions
   */
  getPolarDBXVersions(): Observable<any[]> {
    const req = this.http.get<any>(`${this.baseUrl}/platform/system/polardbx-versions`, { headers: this.getHeaders() })
      .pipe(
        map((res: any) => {
          if (Array.isArray(res)) return res;
          if (Array.isArray(res?.items)) return res.items;
          return [];
        })
      );
    return this.handleRequest(
      req,
      LoadingKeys.SYSTEM,
      '/platform/system/polardbx-versions',
      'GET'
    );
  }

  /**
   * Get platform namespace list (for cluster creation wizard)
   */
  getPlatformNamespaces(): Observable<any[]> {
    const req = this.http.get<any>(`${this.baseUrl}/platform/system/namespaces`, { headers: this.getHeaders() })
      .pipe(
        map((res: any) => {
          if (Array.isArray(res)) return res;
          if (Array.isArray(res?.items)) return res.items;
          return [];
        })
      );
    return this.handleRequest(
      req,
      LoadingKeys.SYSTEM,
      '/platform/system/namespaces',
      'GET'
    );
  }
}
