import { Routes } from '@angular/router';
import { LayoutComponent } from './pages/layout/layout.component';
import { ClusterListComponent } from './pages/cluster-list/cluster-list.component';
import { AuthGuard } from './guards/auth.guard';
import { ParameterTemplateManagementComponent } from './components/parameter-template-management/parameter-template-management.component';
import { SystemTaskManagementComponent } from './components/system-task-management/system-task-management.component';

export const routes: Routes = [
  { 
    path: 'connect', 
    loadComponent: () => import('./pages/connect/connect.component').then(m => m.ConnectComponent)
  },
  {
    path: '',
    component: LayoutComponent,
    canActivate: [AuthGuard],
    children: [
      { path: '', redirectTo: 'clusters', pathMatch: 'full' },
      { path: 'clusters', component: ClusterListComponent },
      { 
        path: 'clusters/:namespace/:name', 
        loadComponent: () => import('./pages/cluster-detail/cluster-detail.component').then(m => m.ClusterDetailComponent)
      },
      {
        path: 'clusters/:namespace/:name/change',
        loadComponent: () => import('./components/cluster-change-wizard/cluster-change-wizard.component').then(m => m.ClusterChangeWizardComponent)
      },
      
      // Backup Management Module
      {
        path: 'backup',
        children: [
          { path: '', redirectTo: 'manual-backups', pathMatch: 'full' },
          { 
            path: 'overview',
            loadComponent: () => import('./components/backup-overview/backup-overview.component').then(m => m.BackupOverviewComponent)
          },
          { 
            path: 'manual-backups', 
            loadComponent: () => import('./components/backup-management/backup-management.component').then(m => m.BackupManagementComponent)
          },
          { 
            path: 'backup-schedules', 
            loadComponent: () => import('./components/backup-schedule-management/backup-schedule-management.component').then(m => m.BackupScheduleManagementComponent)
          },
          { 
            path: 'backup-binlogs', 
            loadComponent: () => import('./components/backup-binlog-management/backup-binlog-management.component').then(m => m.BackupBinlogManagementComponent)
          },
          { 
            path: 'xstore-backups', 
            loadComponent: () => import('./components/xstore-backup-management/xstore-backup-management.component').then(m => m.XStoreBackupManagementComponent)
          }
        ]
      },

      // Top-level aliases for backup subpages
      { path: 'backup-schedules', redirectTo: 'backup/backup-schedules', pathMatch: 'full' },
      { path: 'backup-binlogs', redirectTo: 'backup/backup-binlogs', pathMatch: 'full' },
      { path: 'xstore-backups', redirectTo: 'backup/xstore-backups', pathMatch: 'full' },

      // Recovery Management Module
      {
        path: 'recovery',
        children: [
          { path: '', redirectTo: 'restore-wizard', pathMatch: 'full' },
          { 
            path: 'restore-wizard', 
            loadComponent: () => import('./components/recovery-wizard/recovery-wizard.component').then(m => m.RecoveryWizardComponent)
          },
          { 
            path: 'restore-jobs', 
            loadComponent: () => import('./components/restore-job-management/restore-job-management.component').then(m => m.RestoreJobManagementComponent)
          },
          { 
            path: 'pitr', 
            redirectTo: '/recovery/restore-wizard?mode=pitr',
            pathMatch: 'full'
          }
        ]
      },

      // Aliases for recovery module under /restore
      {
        path: 'restore',
        children: [
          { path: '', redirectTo: 'restore-wizard', pathMatch: 'full' },
          { 
            path: 'restore-wizard', 
            loadComponent: () => import('./components/recovery-wizard/recovery-wizard.component').then(m => m.RecoveryWizardComponent)
          },
          { 
            path: 'restore-jobs', 
            loadComponent: () => import('./components/restore-job-management/restore-job-management.component').then(m => m.RestoreJobManagementComponent)
          },
          { 
            path: 'pitr', 
            redirectTo: '/recovery/restore-wizard?mode=pitr',
            pathMatch: 'full'
          }
        ]
      },

      // Storage Management Module
      {
        path: 'storage',
        children: [
          { path: '', redirectTo: 'xstores', pathMatch: 'full' },
          { 
            path: 'xstores', 
            loadComponent: () => import('./components/xstore-management/xstore-management.component').then(m => m.XStoreManagementComponent)
          },
          { 
            path: 'xstore-followers', 
            loadComponent: () => import('./components/xstore-follower-management/xstore-follower-management.component').then(m => m.XStoreFollowerManagementComponent)
          },
          {
            path: 'xstore-rebuild',
            children: [
              { path: '', redirectTo: 'rebuild', pathMatch: 'full' },
              { 
                path: 'rebuild',
                children: [
                  { path: '', redirectTo: 'tasks', pathMatch: 'full' },
                  { path: 'new', loadComponent: () => import('./components/rebuild-center/rebuild-form.component').then(m => m.RebuildFormComponent) },
                  { path: 'tasks', loadComponent: () => import('./components/rebuild-center/rebuild-task-list.component').then(m => m.RebuildTaskListComponent) },
                  { path: 'tasks/:namespace/:name', loadComponent: () => import('./components/rebuild-center/rebuild-task-detail.component').then(m => m.RebuildTaskDetailComponent) }
                ]
              },
              // Legacy routes for compatibility
              { path: 'health-check', loadComponent: () => import('./components/xstore-rebuild/xstore-rebuild-health-check.component').then(m => m.XStoreRebuildHealthCheckComponent) },
              { path: 'rebuild-follower', loadComponent: () => import('./components/xstore-rebuild/xstore-rebuild-follower.component').then(m => m.XStoreRebuildFollowerComponent) },
              { path: 'rebuild-logger', redirectTo: 'rebuild/new?role=logger', pathMatch: 'full' },
              { path: 'rebuild-learner', redirectTo: 'rebuild/new?role=learner', pathMatch: 'full' }
            ]
          }
        ]
      },

      // Operations Management Module
      {
        path: 'operations',
        children: [
          { path: '', redirectTo: 'monitoring', pathMatch: 'full' },
          {
            path: 'logs',
            loadComponent: () => import('./components/logs-hub/logs-hub.component').then(m => m.LogsHubComponent),
            children: [
              { path: '', redirectTo: 'enable-wizard', pathMatch: 'full' },
              { path: 'overview', loadComponent: () => import('./components/logs-overview/logs-overview.component').then(m => m.LogsOverviewComponent) },
              { path: 'dashboard', loadComponent: () => import('./components/log-service-dashboard/log-service-dashboard.component').then(m => m.LogServiceDashboardComponent) },
              { path: 'collectors', loadComponent: () => import('./components/log-collector-management/log-collector-management.component').then(m => m.LogCollectorManagementComponent) },
              { path: 'strategies', loadComponent: () => import('./components/log-strategy-management/log-strategy-management.component').then(m => m.LogStrategyManagementComponent) },
              { path: 'ilm', loadComponent: () => import('./components/log-collector-ilm/log-collector-ilm.component').then(m => m.LogCollectorIlmComponent) },
              { path: 'search', loadComponent: () => import('./components/logs-query/logs-query.component').then(m => m.LogsQueryComponent) },
              { path: 'install', loadComponent: () => import('./components/log-collector-install/log-collector-install.component').then(m => m.LogCollectorInstallComponent) }
            ]
          },
          { 
            path: 'monitoring',
            loadComponent: () => import('./components/monitoring-hub/monitoring-hub.component').then(m => m.MonitoringHubComponent),
            children: [
              { path: '', redirectTo: 'overview', pathMatch: 'full' },
              { path: 'overview', loadComponent: () => import('./components/monitoring-overview/monitoring-overview.component').then(m => m.MonitoringOverviewComponent) },
              { path: 'config', loadComponent: () => import('./components/monitor-management/monitor-management.component').then(m => m.MonitorManagementComponent) },
              // 旧的安装向导（模拟版）重定向到新的 enable-wizard
              { path: 'install', redirectTo: 'enable-wizard', pathMatch: 'full' },
              { path: 'enable-wizard', loadComponent: () => import('./components/monitoring-installation/monitoring-installation-wizard.component').then(m => m.MonitoringInstallationWizardComponent) },
              { path: 'health', loadComponent: () => import('./components/monitoring-health/monitoring-health.component').then(m => m.MonitoringHealthComponent) },
              { path: 'preflight', loadComponent: () => import('./components/monitoring-preflight/monitoring-preflight.component').then(m => m.MonitoringPreflightComponent) },
              { path: 'dashboards', loadComponent: () => import('./components/monitoring-dashboard-templates/monitoring-dashboard-templates.component').then(m => m.MonitoringDashboardTemplatesComponent) },
              { path: 'alert-templates', loadComponent: () => import('./components/monitoring-alert-rule-templates/monitoring-alert-rule-templates.component').then(m => m.MonitoringAlertRuleTemplatesComponent) },
              { path: 'grafana', loadComponent: () => import('./components/grafana-embed/grafana-embed.component').then(m => m.GrafanaEmbedComponent) },
              { path: 'alerts', loadComponent: () => import('./components/alerts-aggregation/alerts-aggregation.component').then(m => m.AlertsAggregationComponent) },
              { path: 'alerts-mgr', loadComponent: () => import('./components/alerts-management/alerts-management.component').then(m => m.AlertsManagementComponent) },
              { path: 'prometheus-rules', loadComponent: () => import('./components/prometheus-rule-viewer/prometheus-rule-viewer.component').then(m => m.PrometheusRuleViewerComponent) },
              { path: 'alert-receivers', loadComponent: () => import('./components/alert-receiver-wizard/alert-receiver-wizard.component').then(m => m.AlertReceiverWizardComponent) }
            ]
          },
          { path: 'nodes', loadComponent: () => import('./pages/nodes/nodes.component').then(m => m.NodesComponent) },
          { path: 'nodes/:namespace/:name', loadComponent: () => import('./pages/node-detail/node-detail.component').then(m => m.NodeDetailComponent) },
          { path: 'monitors', redirectTo: 'monitoring/config', pathMatch: 'full' },
          { path: 'parameter-templates', component: ParameterTemplateManagementComponent },
          { path: 'system-tasks', component: SystemTaskManagementComponent },
          { path: 'cluster-knobs', loadComponent: () => import('./components/cluster-knobs-management/cluster-knobs-management.component').then(m => m.ClusterKnobsManagementComponent) },
          { path: 'settings', loadComponent: () => import('./components/settings-management/settings-management.component').then(m => m.SettingsManagementComponent) },
          { path: 'helm-values-helper', loadComponent: () => import('./components/helm-values-helper/helm-values-helper.component').then(m => m.HelmValuesHelperComponent) },
          { path: 'runbook-slo', loadComponent: () => import('./components/runbook-slo/runbook-slo.component').then(m => m.RunbookSloComponent) },
          // 旧日志路径重定向到聚合
          { path: 'log-collectors', redirectTo: 'logs/collectors', pathMatch: 'full' },
          { path: 'log-ilm', redirectTo: 'logs/ilm', pathMatch: 'full' },
          { path: 'cluster-logs', redirectTo: 'logs/search', pathMatch: 'full' },
          { path: 'diagnostics', loadComponent: () => import('./components/diagnostics-management/diagnostics-management.component').then(m => m.DiagnosticsManagementComponent) },
          { path: 'prechange-check', loadComponent: () => import('./components/prechange-check/prechange-check.component').then(m => m.PrechangeCheckComponent) },
          // 旧路径重定向
          { path: 'grafana', redirectTo: 'monitoring/grafana', pathMatch: 'full' },
          { path: 'alerts', redirectTo: 'monitoring/alerts', pathMatch: 'full' },
          { path: 'monitoring-wizard', redirectTo: 'monitoring/install', pathMatch: 'full' },
          { path: 'monitoring-health', redirectTo: 'monitoring/health', pathMatch: 'full' },
          { path: 'alerts-management', redirectTo: 'monitoring/alerts-mgr', pathMatch: 'full' }
        ]
      }
    ]
  },
  { path: '**', redirectTo: 'connect' }
];
