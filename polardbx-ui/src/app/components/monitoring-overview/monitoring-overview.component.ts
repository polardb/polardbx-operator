import { Component, OnInit, OnDestroy, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Router } from '@angular/router';
import { Subscription, interval, Subject } from 'rxjs';
import { switchMap, takeUntil } from 'rxjs/operators';

// NG-ZORRO Modules
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzStatisticModule } from 'ng-zorro-antd/statistic';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTimelineModule } from 'ng-zorro-antd/timeline';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzListModule } from 'ng-zorro-antd/list';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzSpaceModule } from 'ng-zorro-antd/space';
import { NzBadgeModule } from 'ng-zorro-antd/badge';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzSpinModule } from 'ng-zorro-antd/spin';

// Services
import { ApiService } from '../../services/api.service';

interface MonitoringSystemStatus {
  loaded: boolean;
  installed: boolean;
  installing: boolean;
  overallHealth: number;
  namespace?: string;
  namespaceExists?: boolean;
  namespaceError?: string;
  installJob?: InstallJobInfo | null;
  components?: ComponentsStatus;
}

interface InstallJobInfo {
  jobName: string;
  namespace: string;
  targetNamespace?: string;
  phase: InstallPhase;
  progress: number;
  steps?: InstallStep[];
  startTime?: string;
  completionTime?: string;
  failureReason?: string;
}

type InstallPhase = 'Pending' | 'Running' | 'Verifying' | 'Succeeded' | 'Failed' | 'ImagePullError';

interface InstallStep {
  name: string;
  status: 'pending' | 'running' | 'success' | 'failed';
  message?: string;
}

interface ComponentsStatus {
  prometheus?: ComponentInfo;
  grafana?: ComponentInfo;
  alertmanager?: ComponentInfo;
}

interface ComponentInfo {
  exists?: boolean;
  ready?: boolean;
  readyReplicas?: number;
  replicas?: number;
  service?: boolean;
  accessUrl?: string;
}

interface MonitoringStatusResponse {
  namespace?: string;
  namespaceExists?: boolean;
  namespaceError?: string;
  components?: ComponentsStatus;
  prerequisites?: {
    crds?: Record<string, unknown>;
  };
}

interface MonitoringBootstrapStatusResponse {
  phase?: InstallPhase;
  startTime?: string;
  completionTime?: string;
  failureReason?: string;
  active?: number;
  succeeded?: number;
  failed?: number;
  conditions?: {
    type?: string;
    status?: string;
    reason?: string;
    message?: string;
    lastTransitionTime?: string;
  }[];
}

type ApiEnvelope<T> = T | { data?: T; success?: boolean; message?: string; error?: unknown };

interface StoredInstallJob {
  jobName: string;
  namespace: string;
  targetNs?: string;
  timestamp?: number;
  expiresAt?: number;
  phase?: InstallPhase;
  failureReason?: string;
}

@Component({
  selector: 'app-monitoring-overview',
  standalone: true,
  imports: [
    CommonModule,
    RouterModule,
    NzCardModule,
    NzIconModule,
    NzButtonModule,
    NzResultModule,
    NzStatisticModule,
    NzProgressModule,
    NzTimelineModule,
    NzAlertModule,
    NzListModule,
    NzTagModule,
    NzSpaceModule,
    NzBadgeModule,
    NzGridModule,
    NzModalModule,
    NzSpinModule
  ],
  template: `
    <div class="monitoring-overview">
      <!-- Loading State -->
      <div *ngIf="!systemStatus.loaded" class="loading-container">
        <nz-spin nzSimple [nzSize]="'large'" nzTip="正在加载监控系统状态..."></nz-spin>
      </div>

      <!-- State 1: Not Installed -->
      <div *ngIf="systemStatus.loaded && !systemStatus.installed && !systemStatus.installing" class="empty-state">
        <nz-result
          nzStatus="info"
          nzTitle="监控系统尚未安装"
          nzSubTitle="按照官方推荐流程先安装监控组件，再为目标集群创建监控对象">
          <div nz-result-extra>
            <button nz-button nzType="primary" nzSize="large" (click)="startInstallation()">
              <i nz-icon nzType="cloud-upload"></i>
              一键安装监控组件
            </button>
            <button nz-button nzType="default" nzSize="large" (click)="goto('preflight')">
              <i nz-icon nzType="safety"></i>
              运行预检查
            </button>
            <button nz-button nzType="link" nzSize="large" (click)="openDocs('install')">
              <i nz-icon nzType="book"></i>
              查看官方安装指南
            </button>
          </div>
        </nz-result>

        <nz-alert
          nzType="warning"
          nzShowIcon
          nzMessage="前置条件检查"
          nzDescription="请确认 Kubernetes ≥ 1.18、Helm 3、PolarDB-X Operator ≥ 1.2.0 且已准备 polardbx-monitor 命名空间，然后再执行监控安装。">
        </nz-alert>

        <nz-alert
          *ngIf="systemStatus.namespaceExists === false"
          nzType="error"
          nzShowIcon
          nzMessage="检测到监控命名空间缺失"
          [nzDescription]="namespaceMissingTpl"
          style="margin-top: 16px; max-width: 900px; margin-left: auto; margin-right: auto;">
        </nz-alert>

        <nz-alert
          *ngIf="systemStatus.namespaceExists !== false && systemStatus.namespaceError"
          nzType="warning"
          nzShowIcon
          nzMessage="无法确认命名空间状态"
          [nzDescription]="systemStatus.namespaceError"
          style="margin-top: 16px; max-width: 900px; margin-left: auto; margin-right: auto;">
        </nz-alert>

        <ng-template #namespaceMissingTpl>
          <p>请先创建命名空间后再启动安装：</p>
          <pre>{{ getNamespaceCreateCommand(systemStatus.namespace || 'polardbx-monitor') }}</pre>
          <button
            nz-button
            nzType="dashed"
            nzSize="small"
            (click)="copyText(getNamespaceCreateCommand(systemStatus.namespace || 'polardbx-monitor'))">
            <i nz-icon nzType="copy"></i>
            复制命令
          </button>
        </ng-template>

        <ng-template #alertmanagerGuideTitle>
          <span class="card-title">
            <i nz-icon nzType="bell"></i>
            <span>Alertmanager 启用指南</span>
          </span>
        </ng-template>
        <nz-card [nzTitle]="alertmanagerGuideTitle" style="margin-top: 16px; max-width: 900px; margin-left: auto; margin-right: auto;">
          <p>默认安装会创建 Alertmanager，请为可运行 Alertmanager 的节点打标签并 patch 实例以确保副本成功调度。</p>
          <pre>{{ getAlertmanagerGuide(systemStatus.namespace) }}</pre>
          <button
            nz-button
            nzType="dashed"
            nzSize="small"
            (click)="copyText(getAlertmanagerGuide(systemStatus.namespace))">
            <i nz-icon nzType="copy"></i>
            复制命令
          </button>
        </nz-card>

        <ng-template #installNotesTitle>
          <span class="card-title">
            <i nz-icon nzType="profile"></i>
            <span>安装前须知</span>
          </span>
        </ng-template>
        <nz-card [nzTitle]="installNotesTitle" style="margin-top: 16px; max-width: 900px; margin-left: auto; margin-right: auto;">
          <ul>
            <li>预计安装时长：3-5 分钟，建议保留页面以跟踪进度</li>
            <li>核心组件：Prometheus Operator、Prometheus、Grafana、Alertmanager、Node Exporter、Kube State Metrics</li>
            <li>默认命名空间：<code>polardbx-monitor</code>，可在启用向导中调整</li>
            <li>网络要求：能够访问 <code>https://polardbx-charts.oss-cn-beijing.aliyuncs.com</code> 拉取 Helm Chart，或预先配置镜像源</li>
            <li>官方流程：先安装监控组件，再创建 <code>PolarDBXMonitor</code>（企业版）或 <code>ServiceMonitor</code>（标准版）对象</li>
          </ul>
        </nz-card>
      </div>

      <!-- State 2: Installing -->
      <div *ngIf="systemStatus.loaded && systemStatus.installing" class="installing-state">
        <ng-template #installingTitle>
          <span class="card-title">
            <i nz-icon nzType="rocket"></i>
            <span>正在安装监控系统</span>
          </span>
        </ng-template>
        <nz-card [nzTitle]="installingTitle" style="max-width: 900px; margin: 0 auto;">
          <nz-progress
            [nzPercent]="systemStatus.installJob?.progress || 0"
            nzStatus="active"
            [nzShowInfo]="true"
            style="margin-bottom: 24px;">
          </nz-progress>

          <nz-timeline *ngIf="systemStatus.installJob?.steps?.length">
            <nz-timeline-item
              *ngFor="let step of systemStatus.installJob?.steps"
              [nzColor]="getStepColor(step.status)">
              <p><strong>{{ step.name }}</strong></p>
              <p class="step-message" *ngIf="step.message">{{ step.message }}</p>
            </nz-timeline-item>
          </nz-timeline>

          <div class="actions" style="margin-top: 24px; display: flex; gap: 8px;">
            <button nz-button nzType="default" (click)="viewJobLogs()">
              <i nz-icon nzType="file-text"></i>
              查看安装日志
            </button>
            <button nz-button nzType="dashed" (click)="refreshStatus()">
              <i nz-icon nzType="reload"></i>
              刷新状态
            </button>
          </div>
        </nz-card>

        <nz-alert
          nzType="info"
          nzMessage="安装进行中"
          nzDescription="请勿关闭页面。安装完成后会自动更新状态。"
          nzShowIcon
          style="margin-top: 16px; max-width: 900px; margin-left: auto; margin-right: auto;">
        </nz-alert>
      </div>

      <!-- State 3: Installed Successfully -->
      <div *ngIf="systemStatus.loaded && systemStatus.installed && !systemStatus.installing" class="installed-state">
        <!-- System Overview Card -->
        <ng-template #systemStatusTitle>
          <span class="card-title">
            <i nz-icon nzType="bar-chart"></i>
            <span>监控系统状态</span>
          </span>
        </ng-template>
        <nz-card [nzTitle]="systemStatusTitle" [nzExtra]="healthBadgeTemplate" style="margin-bottom: 16px;">
          <nz-row [nzGutter]="16">
            <nz-col [nzSpan]="6">
              <nz-statistic
                nzTitle="健康度"
                [nzValue]="systemStatus.overallHealth"
                nzSuffix="%"
                [nzValueStyle]="{ color: getHealthColor(systemStatus.overallHealth) }">
              </nz-statistic>
            </nz-col>
            <nz-col [nzSpan]="6">
              <nz-statistic nzTitle="运行组件" [nzValue]="getRunningComponentsCount()" nzSuffix="/ 3"></nz-statistic>
            </nz-col>
            <nz-col [nzSpan]="6">
              <nz-statistic nzTitle="命名空间" [nzValue]="systemStatus.namespace || 'polardbx-monitor'"></nz-statistic>
            </nz-col>
            <nz-col [nzSpan]="6">
              <nz-statistic nzTitle="状态" [nzValue]="systemStatus.overallHealth > 80 ? '良好' : '异常'"></nz-statistic>
            </nz-col>
          </nz-row>
        </nz-card>

        <ng-template #healthBadgeTemplate>
          <nz-badge
            [nzStatus]="systemStatus.overallHealth > 80 ? 'success' : 'error'"
            [nzText]="systemStatus.overallHealth > 80 ? '健康' : '异常'">
          </nz-badge>
        </ng-template>

        <!-- Components Status -->
        <ng-template #componentsStatusTitle>
          <span class="card-title">
            <i nz-icon nzType="tool"></i>
            <span>组件状态</span>
          </span>
        </ng-template>
        <nz-card [nzTitle]="componentsStatusTitle" style="margin-bottom: 16px;">
          <nz-list nzBordered>
            <nz-list-item *ngIf="systemStatus.components?.prometheus">
              <ng-template #prometheusAvatar>
                <i nz-icon nzType="line-chart" class="component-avatar-icon prometheus-icon"></i>
              </ng-template>
              <nz-list-item-meta
                [nzAvatar]="prometheusAvatar"
                nzTitle="Prometheus"
                nzDescription="时序数据库和监控系统">
              </nz-list-item-meta>
              <ul nz-list-item-actions>
                <nz-list-item-action>
                  <nz-tag [nzColor]="systemStatus.components?.prometheus?.ready ? 'success' : 'error'">
                    {{ systemStatus.components?.prometheus?.ready ? '运行中' : '异常' }}
                  </nz-tag>
                </nz-list-item-action>
                <nz-list-item-action>
                  <span>{{ systemStatus.components?.prometheus?.readyReplicas }}/{{ systemStatus.components?.prometheus?.replicas }} Pods</span>
                </nz-list-item-action>
                <nz-list-item-action>
                  <button nz-button nzType="link" nzSize="small" (click)="openComponent('prometheus', systemStatus.components?.prometheus?.accessUrl)">
                    <i nz-icon nzType="link"></i> 访问
                  </button>
                </nz-list-item-action>
              </ul>
            </nz-list-item>

            <nz-list-item *ngIf="systemStatus.components?.grafana">
              <ng-template #grafanaAvatar>
                <i nz-icon nzType="area-chart" class="component-avatar-icon grafana-icon"></i>
              </ng-template>
              <nz-list-item-meta
                [nzAvatar]="grafanaAvatar"
                nzTitle="Grafana"
                nzDescription="可视化和仪表板">
              </nz-list-item-meta>
              <ul nz-list-item-actions>
                <nz-list-item-action>
                  <nz-tag [nzColor]="systemStatus.components?.grafana?.ready ? 'success' : 'error'">
                    {{ systemStatus.components?.grafana?.ready ? '运行中' : '异常' }}
                  </nz-tag>
                </nz-list-item-action>
                <nz-list-item-action>
                  <span>{{ systemStatus.components?.grafana?.readyReplicas }}/{{ systemStatus.components?.grafana?.replicas }} Pods</span>
                </nz-list-item-action>
                <nz-list-item-action>
                  <button nz-button nzType="link" nzSize="small" (click)="openComponent('grafana', systemStatus.components?.grafana?.accessUrl)">
                    <i nz-icon nzType="link"></i> 访问
                  </button>
                </nz-list-item-action>
              </ul>
            </nz-list-item>

            <nz-list-item *ngIf="systemStatus.components?.alertmanager">
              <ng-template #alertmanagerAvatar>
                <i nz-icon nzType="bell" class="component-avatar-icon alertmanager-icon"></i>
              </ng-template>
              <nz-list-item-meta
                [nzAvatar]="alertmanagerAvatar"
                nzTitle="Alertmanager"
                nzDescription="告警管理和通知">
              </nz-list-item-meta>
              <ul nz-list-item-actions>
                <nz-list-item-action>
                  <nz-tag [nzColor]="systemStatus.components?.alertmanager?.ready ? 'success' : 'error'">
                    {{ systemStatus.components?.alertmanager?.ready ? '运行中' : '异常' }}
                  </nz-tag>
                </nz-list-item-action>
                <nz-list-item-action>
                  <span>{{ systemStatus.components?.alertmanager?.readyReplicas }}/{{ systemStatus.components?.alertmanager?.replicas }} Pods</span>
                </nz-list-item-action>
                <nz-list-item-action>
                  <button nz-button nzType="link" nzSize="small" (click)="openComponent('alertmanager', systemStatus.components?.alertmanager?.accessUrl)">
                    <i nz-icon nzType="link"></i> 访问
                  </button>
                </nz-list-item-action>
              </ul>
            </nz-list-item>
          </nz-list>
        </nz-card>

        <nz-alert
          *ngIf="systemStatus.components?.alertmanager && !systemStatus.components?.alertmanager?.ready"
          nzType="warning"
          nzShowIcon
          nzMessage="Alertmanager 未完全启用"
          [nzDescription]="alertmanagerGuideTpl"
          style="margin-bottom: 16px;">
        </nz-alert>

        <ng-template #alertmanagerGuideTpl>
          <p>请执行以下命令为 Alertmanager 指定节点标签并重新调度：</p>
          <pre>{{ getAlertmanagerGuide(systemStatus.namespace) }}</pre>
          <button
            nz-button
            nzType="dashed"
            nzSize="small"
            (click)="copyText(getAlertmanagerGuide(systemStatus.namespace))">
            <i nz-icon nzType="copy"></i>
            复制命令
          </button>
        </ng-template>

        <!-- Quick Actions -->
        <ng-template #quickActionsTitle>
          <span class="card-title">
            <i nz-icon nzType="aim"></i>
            <span>快速入口</span>
          </span>
        </ng-template>
        <nz-card [nzTitle]="quickActionsTitle" style="margin-bottom: 16px;">
          <nz-row [nzGutter]="16">
            <nz-col [nzSpan]="8">
              <nz-card nzHoverable class="quick-action-card" (click)="openGrafana()">
                <div class="quick-action">
                  <i nz-icon nzType="dashboard" style="font-size: 32px; color: #1890ff;"></i>
                  <h4>Grafana 仪表板</h4>
                  <p>查看监控数据和可视化图表</p>
                </div>
              </nz-card>
            </nz-col>
            <nz-col [nzSpan]="8">
              <nz-card nzHoverable class="quick-action-card" (click)="openPrometheus()">
                <div class="quick-action">
                  <i nz-icon nzType="line-chart" style="font-size: 32px; color: #52c41a;"></i>
                  <h4>Prometheus 查询</h4>
                  <p>执行 PromQL 查询和表达式</p>
                </div>
              </nz-card>
            </nz-col>
            <nz-col [nzSpan]="8">
              <nz-card nzHoverable class="quick-action-card" (click)="openAlertmanager()">
                <div class="quick-action">
                  <i nz-icon nzType="bell" style="font-size: 32px; color: #faad14;"></i>
                  <h4>Alertmanager</h4>
                  <p>管理告警规则和通知</p>
                </div>
              </nz-card>
            </nz-col>
            <nz-col [nzSpan]="8">
              <nz-card nzHoverable class="quick-action-card" (click)="goto('dashboards')">
                <div class="quick-action">
                  <i nz-icon nzType="appstore" style="font-size: 32px; color: #722ed1;"></i>
                  <h4>Dashboard 模板</h4>
                  <p>导入官方仪表板，快速获得可视化</p>
                </div>
              </nz-card>
            </nz-col>
          </nz-row>
        </nz-card>

        <!-- System Management -->
        <ng-template #systemManagementTitle>
          <span class="card-title">
            <i nz-icon nzType="setting"></i>
            <span>系统管理</span>
          </span>
        </ng-template>
        <nz-card [nzTitle]="systemManagementTitle" style="margin-bottom: 16px;">
          <nz-space>
            <button *nzSpaceItem nz-button nzType="default" (click)="goto('health')">
              <i nz-icon nzType="safety"></i>
              健康检查
            </button>
            <button *nzSpaceItem nz-button nzType="default" (click)="goto('config')">
              <i nz-icon nzType="setting"></i>
              监控配置
            </button>
            <button *nzSpaceItem nz-button nzType="default" (click)="reconfigure()">
              <i nz-icon nzType="edit"></i>
              重新配置
            </button>
            <button *nzSpaceItem nz-button nzDanger (click)="confirmUninstall()">
              <i nz-icon nzType="delete"></i>
              卸载监控系统
            </button>
          </nz-space>
        </nz-card>
      </div>

      <!-- State 4: Installation Failed -->
      <div *ngIf="systemStatus.loaded && systemStatus.installJob?.phase === 'Failed'" class="failed-state">
        <ng-template #installFailedTitle>
          <span class="card-title">
            <i nz-icon nzType="close-circle"></i>
            <span>监控系统安装失败</span>
          </span>
        </ng-template>
        <nz-result
          nzStatus="error"
          [nzTitle]="installFailedTitle"
          [nzSubTitle]="systemStatus.installJob?.failureReason || '安装过程中发生错误'">
          <div nz-result-content>
            <nz-alert
              nzType="error"
              nzMessage="错误详情"
              [nzDescription]="errorTemplate"
              nzShowIcon>
            </nz-alert>

            <ng-template #errorTemplate>
              <p><strong>阶段：</strong>安装监控组件</p>
              <p><strong>时间：</strong>{{ systemStatus.installJob?.startTime || '未知' }}</p>
              <p><strong>错误：</strong>{{ systemStatus.installJob?.failureReason || '未知错误' }}</p>
            </ng-template>

            <nz-alert
              *ngIf="isBackoffLimitFailure()"
              nzType="warning"
              nzMessage="安装任务达到重试上限"
              [nzDescription]="backoffGuideTpl"
              nzShowIcon
              class="failure-guidance-alert">
            </nz-alert>

            <ng-template #backoffGuideTpl>
              <div class="failure-guidance">
                <p><strong>问题原因：</strong>安装 Job 多次重试仍失败，Kubernetes 已停止继续尝试。</p>
                <p><strong>建议操作：</strong></p>
                <ol>
                  <li>
                    查看 Job 事件并定位失败阶段
                    <div class="command-block">
                      <pre>{{ getJobDescribeCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyText(getJobDescribeCommand())">
                        <i nz-icon nzType="copy"></i>
                        复制
                      </button>
                    </div>
                  </li>
                  <li>
                    检查最近失败 Pod 的详细日志
                    <div class="command-block">
                      <pre>{{ getJobFailedPodLogsCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyText(getJobFailedPodLogsCommand())">
                        <i nz-icon nzType="copy"></i>
                        复制
                      </button>
                    </div>
                  </li>
                  <li>
                    修复问题后清理旧 Job 并重新触发安装
                    <div class="command-block">
                      <pre>{{ getJobDeleteCommand() }}</pre>
                      <button nz-button nzType="dashed" nzSize="small" (click)="copyText(getJobDeleteCommand())">
                        <i nz-icon nzType="copy"></i>
                        复制
                      </button>
                    </div>
                  </li>
                </ol>
                <p>完成上述操作后，可点击下方「重新安装」按钮再次发起安装任务。</p>
              </div>
            </ng-template>
          </div>

          <div nz-result-extra style="margin-top: 24px;">
            <button nz-button nzType="primary" (click)="retryInstallation()">
              <i nz-icon nzType="reload"></i>
              重新安装
            </button>
            <button nz-button nzType="default" (click)="viewJobLogs()">
              <i nz-icon nzType="file-text"></i>
              查看完整日志
            </button>
            <button nz-button nzType="default" (click)="goto('enable-wizard')">
              <i nz-icon nzType="left"></i>
              返回配置
            </button>
          </div>
        </nz-result>
      </div>
    </div>
  `,
  styles: [`
    .monitoring-overview {
      padding: 16px;
      background: #f5f5f5;
      min-height: calc(100vh - 64px);
    }

    .loading-container {
      display: flex;
      justify-content: center;
      align-items: center;
      min-height: 400px;
    }

    .empty-state,
    .installing-state,
    .installed-state,
    .failed-state {
      max-width: 1200px;
      margin: 0 auto;
    }

    .step-message {
      margin: 4px 0 0 0;
      color: rgba(0, 0, 0, 0.45);
      font-size: 12px;
    }

    .quick-action-card {
      cursor: pointer;
      transition: all 0.3s;
    }

    .quick-action-card:hover {
      transform: translateY(-4px);
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    }

    .quick-action {
      text-align: center;
      padding: 16px;
    }

    .quick-action h4 {
      margin: 12px 0 8px 0;
      color: rgba(0, 0, 0, 0.85);
      font-size: 16px;
      font-weight: 500;
    }

    .quick-action p {
      margin: 0;
      color: rgba(0, 0, 0, 0.45);
      font-size: 14px;
    }

    .card-title {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }

    .card-title i[nz-icon] {
      font-size: 18px;
      color: #1890ff;
    }

    .command-block {
      position: relative;
      background: #f6f8fa;
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
      margin-top: 8px;
    }

    .command-block pre {
      margin: 0;
      font-family: 'SFMono-Regular', 'Monaco', 'Menlo', 'Courier New', monospace;
      font-size: 13px;
      line-height: 1.4;
      color: #24292e;
      word-wrap: break-word;
      white-space: pre-wrap;
    }

    .command-block button {
      position: absolute;
      top: 8px;
      right: 8px;
    }

    .failure-guidance-alert {
      margin-top: 16px;
    }

    .failure-guidance {
      font-size: 13px;
      line-height: 1.7;
      text-align: left;
    }

    .failure-guidance ol {
      margin: 12px 0;
      padding-left: 18px;
    }

    .failure-guidance li {
      margin-bottom: 12px;
    }

    .failure-guidance p {
      margin: 4px 0;
    }

    .component-avatar-icon {
      font-size: 20px;
    }

    .prometheus-icon {
      color: #52c41a;
    }

    .grafana-icon {
      color: #722ed1;
    }

    .alertmanager-icon {
      color: #faad14;
    }

    @media (max-width: 768px) {
      .monitoring-overview {
        padding: 8px;
      }
    }
  `]
})
export class MonitoringOverviewComponent implements OnInit, OnDestroy {
  private readonly router = inject(Router);
  private readonly api = inject(ApiService);
  private readonly modal = inject(NzModalService);
  private readonly message = inject(NzMessageService);

  systemStatus: MonitoringSystemStatus = {
    loaded: false,
    installed: false,
    installing: false,
    overallHealth: 0,
    installJob: null
  };

  private readonly destroy$ = new Subject<void>();
  private pollingSubscription: Subscription | null = null;

  ngOnInit(): void {
    this.loadSystemStatus();
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
    this.stopPolling();
  }

  getNamespaceCreateCommand(namespace?: string): string {
    const ns = (namespace || '').trim() || 'polardbx-monitor';
    return `kubectl create namespace ${ns}`;
  }

  getAlertmanagerGuide(namespace?: string): string {
    const ns = (namespace || '').trim() || 'polardbx-monitor';
    return `# 为运行 Alertmanager 的节点打标签
kubectl label node <node-name> polardbx.com/alertmanager-node=true --overwrite

# 将 Alertmanager 固定到带标签的节点
kubectl patch alertmanager alertmanager-main -n ${ns} --type merge -p '{"spec":{"nodeSelector":{"polardbx.com/alertmanager-node":"true"}}}'`;
  }

  copyText(value: string, success = '命令已复制到剪贴板'): void {
    navigator.clipboard.writeText(value).then(() => {
      this.message.success(success);
    }).catch(() => {
      this.message.error('复制失败，请手动复制');
    });
  }

  isBackoffLimitFailure(): boolean {
    const reason = this.systemStatus.installJob?.failureReason;
    if (!reason) {
      return false;
    }
    const normalized = reason.toLowerCase();
    return normalized.includes('backoff') || normalized.includes('backofflimit') || normalized.includes('重试次数');
  }

  getJobDescribeCommand(): string {
    const ns = this.getInstallJobNamespace();
    const jobName = this.getInstallJobName();
    return `kubectl describe job -n ${ns} ${jobName}`;
  }

  getJobFailedPodLogsCommand(): string {
    const ns = this.getInstallJobNamespace();
    const jobName = this.getInstallJobName();
    return `kubectl logs -n ${ns} $(kubectl get pods -n ${ns} -l job-name=${jobName} -o name | head -1) --previous`;
  }

  getJobDeleteCommand(): string {
    const ns = this.getInstallJobNamespace();
    const jobName = this.getInstallJobName();
    return `kubectl delete job -n ${ns} ${jobName}`;
  }

  private getInstallJobNamespace(): string {
    return this.systemStatus.installJob?.namespace || this.systemStatus.namespace || 'polardbx-monitor';
  }

  private getInstallJobName(): string {
    return this.systemStatus.installJob?.jobName || 'polardbx-monitor-bootstrap';
  }

  /**
   * Load monitoring system status
   */
  private loadSystemStatus(): void {
    this.api.getMonitoringStatus().subscribe({
      next: (response) => {
        const status = this.unwrapApiData(response as ApiEnvelope<MonitoringStatusResponse>);
        const components = status.components ?? {};
        const namespace = status.namespace;
        const installed = this.hasInstalledComponents(components);
        const namespaceExists = status.namespaceExists !== false;
        const namespaceError = status.namespaceError;

        this.checkOngoingInstallation(installed, components, namespace, namespaceExists, namespaceError);
      },
      error: (error) => {
        console.error('Failed to load monitoring status:', error);
        this.systemStatus = {
          loaded: true,
          installed: false,
          installing: false,
          overallHealth: 0,
          namespace: this.systemStatus.namespace,
          namespaceExists: this.systemStatus.namespaceExists,
          namespaceError: this.systemStatus.namespaceError,
          components: this.systemStatus.components,
          installJob: null
        };
        this.message.error(`加载监控状态失败: ${this.getErrorMessage(error)}`);
      }
    });
  }

  /**
   * Check for ongoing installation jobs
   */
  private checkOngoingInstallation(
    installed: boolean,
    components: ComponentsStatus | undefined,
    namespace?: string,
    namespaceExists?: boolean,
    namespaceError?: string
  ): void {
    const jobInfo = this.safeParseJobInfo(localStorage.getItem('polardbx-monitor-install-job'));

    if (!jobInfo) {
      this.setInstalledStatus(installed, components, namespace, namespaceExists, namespaceError);
      return;
    }

    if (jobInfo.expiresAt && Date.now() > jobInfo.expiresAt) {
      localStorage.removeItem('polardbx-monitor-install-job');
      this.setInstalledStatus(installed, components, namespace, namespaceExists, namespaceError);
      return;
    }

    this.api.monitoringBootstrapStatus(jobInfo.jobName, jobInfo.namespace).subscribe({
      next: (jobStatus: MonitoringBootstrapStatusResponse) => {
        const phase = jobStatus.phase ?? 'Pending';
        const parsedJob = this.parseInstallJob(jobStatus, jobInfo);
        const effectiveNamespace = namespace ?? jobInfo.targetNs ?? jobInfo.namespace;

        localStorage.setItem('polardbx-monitor-install-job', JSON.stringify({
          ...jobInfo,
          phase,
          failureReason: jobStatus.failureReason,
          timestamp: Date.now()
        }));

        if (phase === 'Running' || phase === 'Pending' || phase === 'Verifying') {
          this.systemStatus = {
            loaded: true,
            installed: false,
            installing: true,
            overallHealth: 0,
            namespace: effectiveNamespace,
            namespaceExists,
            namespaceError,
            components,
            installJob: parsedJob
          };
          this.startPolling(parsedJob, jobInfo);
          return;
        }

        if (phase === 'Succeeded') {
          localStorage.removeItem('polardbx-monitor-install-job');
          this.message.success('监控系统安装成功！');
          setTimeout(() => this.loadSystemStatus(), 1500);
          return;
        }

        // Failed or ImagePullError
        this.systemStatus = {
          loaded: true,
          installed: false,
          installing: false,
          overallHealth: 0,
          namespace: effectiveNamespace,
          namespaceExists,
          namespaceError,
          components,
          installJob: parsedJob
        };
      },
      error: (error) => {
        console.warn('监控安装任务状态查询失败，回退至当前组件状态:', error);
        localStorage.removeItem('polardbx-monitor-install-job');
        this.setInstalledStatus(installed, components, namespace, namespaceExists, namespaceError);
      }
    });
  }

  /**
   * Set installed status
   */
  private setInstalledStatus(
    installed: boolean,
    components: ComponentsStatus | undefined,
    namespace?: string,
    namespaceExists?: boolean,
    namespaceError?: string
  ): void {
    this.stopPolling();
    const normalizedComponents = components ?? {};
    const resolvedNamespace = namespace ?? this.systemStatus.namespace;
    const resolvedNamespaceExists = typeof namespaceExists === 'boolean'
      ? namespaceExists
      : this.systemStatus.namespaceExists;
    let resolvedNamespaceError = namespaceError ?? this.systemStatus.namespaceError;
    if (resolvedNamespaceExists) {
      resolvedNamespaceError = undefined;
    }
    this.systemStatus = {
      loaded: true,
      installed,
      installing: false,
      components: normalizedComponents,
      namespace: resolvedNamespace,
      namespaceExists: resolvedNamespaceExists,
      namespaceError: resolvedNamespaceError,
      overallHealth: installed ? this.calculateOverallHealth(normalizedComponents) : 0,
      installJob: null
    };
  }

  /**
   * Parse install job info
   */
  private parseInstallJob(
    status: MonitoringBootstrapStatusResponse,
    jobInfo: StoredInstallJob
  ): InstallJobInfo {
    const phase = status.phase ?? 'Pending';
    return {
      jobName: jobInfo.jobName,
      namespace: jobInfo.namespace,
      targetNamespace: jobInfo.targetNs,
      phase,
      progress: this.calculateProgress(status),
      steps: this.extractInstallSteps(status),
      startTime: status.startTime,
      completionTime: status.completionTime,
      failureReason: status.failureReason ?? jobInfo.failureReason
    };
  }

  /**
   * Calculate progress based on status
   */
  private calculateProgress(status: MonitoringBootstrapStatusResponse): number {
    const phase = status.phase ?? 'Pending';

    if (phase === 'Succeeded') {
      return 100;
    }
    if (phase === 'Verifying') {
      return 90;
    }
    if (phase === 'Pending') {
      return 10;
    }
    if (phase === 'Failed' || phase === 'ImagePullError') {
      const succeeded = status.succeeded ?? 0;
      return succeeded > 0 ? 95 : 50;
    }
    if (phase === 'Running') {
      if (status.startTime) {
        const elapsed = Date.now() - new Date(status.startTime).getTime();
        const estimatedTotal = 5 * 60 * 1000; // 5 minutes
        const progress = Math.min(95, 15 + (elapsed / estimatedTotal) * 80);
        return Math.max(20, Math.round(progress));
      }
      return 40;
    }

    return 30;
  }

  /**
   * Extract install steps from job status
   */
  private extractInstallSteps(status: MonitoringBootstrapStatusResponse): InstallStep[] {
    const phase = status.phase ?? 'Pending';
    const steps: InstallStep[] = [
      { name: 'Job 已创建', status: 'success' },
      { name: 'Helm Chart 准备', status: phase === 'Pending' ? 'pending' : 'success' },
      { name: '监控组件安装', status: 'pending' },
      { name: '配置应用', status: 'pending' },
      { name: '健康检查', status: 'pending' }
    ];

    if (phase === 'Running') {
      steps[2].status = 'running';
      steps[2].message = '正在部署 Prometheus、Grafana、Alertmanager...';
    } else if (phase === 'Verifying') {
      steps[2].status = 'success';
      steps[3].status = 'running';
      steps[3].message = '正在执行配置与校验...';
    } else if (phase === 'Succeeded') {
      steps.forEach(step => (step.status = 'success'));
      steps[4].message = '监控组件运行正常';
    } else if (phase === 'Failed' || phase === 'ImagePullError') {
      steps[2].status = 'failed';
      steps[2].message = status.failureReason || '部署失败';
    }

    return steps;
  }

  /**
   * Calculate overall health
   */
  private calculateOverallHealth(components: ComponentsStatus | undefined): number {
    if (!components) {
      return 0;
    }

    const tracked = [components.prometheus, components.grafana, components.alertmanager];
    const existing = tracked.filter(component => component?.exists);

    if (existing.length === 0) {
      return 0;
    }

    const healthy = existing.filter(component => component?.ready).length;
    return Math.round((healthy / existing.length) * 100);
  }

  /**
   * Start polling for installation status
   */
  private startPolling(job: InstallJobInfo, storedJob: StoredInstallJob): void {
    this.stopPolling();

    this.pollingSubscription = interval(3000)
      .pipe(
        switchMap(() => this.api.monitoringBootstrapStatus(job.jobName, job.namespace)),
        takeUntil(this.destroy$)
      )
      .subscribe({
        next: (status: MonitoringBootstrapStatusResponse) => {
          const updatedJob = this.parseInstallJob(status, storedJob);
          const phase = updatedJob.phase;
          const effectiveNamespace = this.systemStatus.namespace ?? storedJob.targetNs ?? storedJob.namespace;

          this.systemStatus = {
            ...this.systemStatus,
            loaded: true,
            namespace: effectiveNamespace,
            installJob: updatedJob,
            installing: phase === 'Running' || phase === 'Pending' || phase === 'Verifying',
            installed: phase === 'Succeeded'
          };

          if (phase === 'Succeeded') {
            localStorage.removeItem('polardbx-monitor-install-job');
            this.stopPolling();
            this.message.success('监控系统安装成功！');
            setTimeout(() => this.loadSystemStatus(), 1500);
            return;
          }

          const updatedStoredJob: StoredInstallJob = {
            ...storedJob,
            phase,
            failureReason: updatedJob.failureReason,
            timestamp: Date.now()
          };

          if (phase === 'Failed' || phase === 'ImagePullError') {
            this.stopPolling();
            localStorage.setItem('polardbx-monitor-install-job', JSON.stringify(updatedStoredJob));
            this.message.error('监控系统安装失败');
            return;
          }

          localStorage.setItem('polardbx-monitor-install-job', JSON.stringify(updatedStoredJob));
        },
        error: (error) => {
          console.error('Polling error:', error);
          this.stopPolling();
          this.message.warning('安装状态轮询失败，请稍后手动刷新');
        }
      });
  }

  private stopPolling(): void {
    if (this.pollingSubscription) {
      this.pollingSubscription.unsubscribe();
      this.pollingSubscription = null;
    }
  }

  /**
   * Get step color for timeline
   */
  getStepColor(status: string): string {
    switch (status) {
      case 'success': return 'green';
      case 'running': return 'blue';
      case 'failed': return 'red';
      default: return 'gray';
    }
  }

  /**
   * Get health color
   */
  getHealthColor(health: number): string {
    if (health >= 80) return '#52c41a';
    if (health >= 50) return '#faad14';
    return '#ff4d4f';
  }

  /**
   * Get running components count
   */
  getRunningComponentsCount(): number {
    let count = 0;
    if (this.systemStatus.components?.prometheus?.ready) count++;
    if (this.systemStatus.components?.grafana?.ready) count++;
    if (this.systemStatus.components?.alertmanager?.ready) count++;
    return count;
  }

  private unwrapApiData<T>(response: ApiEnvelope<T>): T {
    if (response && typeof response === 'object' && 'data' in response) {
      const { data } = response as { data?: T };
      if (data !== undefined) {
        return data;
      }
    }
    return response as T;
  }

  private getErrorMessage(error: unknown): string {
    if (!error) {
      return '未知错误';
    }
    if (typeof error === 'string') {
      return error;
    }
    if (error instanceof Error) {
      return error.message;
    }
    if (typeof error === 'object') {
      const errObj = error as { message?: string; error?: unknown; statusText?: string };
      if (typeof errObj.message === 'string' && errObj.message.trim()) {
        return errObj.message;
      }
      if (errObj.error && typeof errObj.error === 'object') {
        const nested = errObj.error as { message?: string; error?: string };
        if (typeof nested.message === 'string' && nested.message.trim()) {
          return nested.message;
        }
        if (typeof nested.error === 'string' && nested.error.trim()) {
          return nested.error;
        }
      }
      if (typeof errObj.statusText === 'string' && errObj.statusText.trim()) {
        return errObj.statusText;
      }
    }
    return '未知错误';
  }

  private hasInstalledComponents(components?: ComponentsStatus): boolean {
    if (!components) {
      return false;
    }
    return Boolean(
      components.prometheus?.exists ||
      components.grafana?.exists ||
      components.alertmanager?.exists
    );
  }

  private safeParseJobInfo(raw: string | null): StoredInstallJob | null {
    if (!raw) {
      return null;
    }
    try {
      const parsed = JSON.parse(raw) as StoredInstallJob;
      if (parsed?.jobName && parsed?.namespace) {
        return parsed;
      }
    } catch (error) {
      console.warn('解析监控安装任务信息失败:', error);
    }
    localStorage.removeItem('polardbx-monitor-install-job');
    return null;
  }

  /**
   * Navigate to a path
   */
  goto(path: string): void {
    this.router.navigate([`/operations/monitoring/${path}`]);
  }

  /**
   * Start installation
   */
  startInstallation(): void {
    this.router.navigate(['/operations/monitoring/enable-wizard']);
  }

  openDocs(section: 'install' | 'existing'): void {
    const links: Record<typeof section, string> = {
      install: 'https://doc.polardbx.com/zh/operator/ops/monitor/1-monitor-install.html',
      existing: 'https://doc.polardbx.com/zh/operator/ops/monitor/2-monitor-cluster-exist.html'
    } as const;
    const url = links[section];
    window.open(url, '_blank');
  }

  /**
   * Refresh status
   */
  refreshStatus(): void {
    this.message.info('正在刷新状态...');
    this.stopPolling();
    this.loadSystemStatus();
  }

  /**
   * View job logs
   */
  viewJobLogs(): void {
    const jobInfo = this.systemStatus.installJob;
    if (!jobInfo) return;

    const logCommand = `kubectl logs -n ${jobInfo.namespace} job/${jobInfo.jobName} --follow`;
    
    this.modal.info({
      nzTitle: '安装日志',
      nzContent: `
        <p>使用以下命令查看实时日志：</p>
        <pre style="background: #f5f5f5; padding: 12px; border-radius: 4px; overflow-x: auto;">${logCommand}</pre>
        <p style="margin-top: 12px;">或者在浏览器开发者工具中查看网络请求。</p>
      `,
      nzWidth: 600
    });
  }

  /**
   * Open component
   */
  openComponent(name: string, url?: string): void {
    if (!url) {
      this.message.warning(`${name} 访问地址未配置`);
      return;
    }

    if (url.startsWith('port-forward')) {
      this.modal.info({
        nzTitle: '访问 ' + name,
        nzContent: `
          <p>该服务使用 ClusterIP 类型，需要通过 port-forward 访问：</p>
          <pre style="background: #f5f5f5; padding: 12px; border-radius: 4px;">${url}</pre>
        `,
        nzWidth: 600
      });
    } else if (url.startsWith('NodePort:')) {
      const port = url.split(':')[1]?.trim();
      this.modal.info({
        nzTitle: '访问 ' + name,
        nzContent: `
          <p>该服务使用 NodePort 类型，请使用以下地址访问：</p>
          <p><strong>http://&lt;node-ip&gt;:${port}</strong></p>
          <p style="margin-top: 12px; color: #666;">将 &lt;node-ip&gt; 替换为您的 Kubernetes 节点 IP</p>
        `,
        nzWidth: 600
      });
    } else {
      window.open(url, '_blank');
    }
  }

  /**
   * Open Grafana
   */
  openGrafana(): void {
    const url = this.systemStatus.components?.grafana?.accessUrl;
    this.openComponent('Grafana', url);
  }

  /**
   * Open Prometheus
   */
  openPrometheus(): void {
    const url = this.systemStatus.components?.prometheus?.accessUrl;
    this.openComponent('Prometheus', url);
  }

  /**
   * Open Alertmanager
   */
  openAlertmanager(): void {
    const url = this.systemStatus.components?.alertmanager?.accessUrl;
    this.openComponent('Alertmanager', url);
  }

  /**
   * Reconfigure
   */
  reconfigure(): void {
    this.modal.confirm({
      nzTitle: '确认重新配置？',
      nzContent: '这将打开配置向导，您可以修改监控系统的配置。',
      nzOnOk: () => {
        this.router.navigate(['/operations/monitoring/enable-wizard']);
      }
    });
  }

  /**
   * Confirm uninstall
   */
  confirmUninstall(): void {
    this.modal.confirm({
      nzTitle: '确认卸载监控系统？',
      nzContent: `
        <p>这将删除所有监控组件和数据！</p>
        <p><strong>警告：</strong>此操作不可恢复，请确保已备份重要数据。</p>
      `,
      nzOkText: '确认卸载',
      nzOkDanger: true,
      nzCancelText: '取消',
      nzOnOk: () => {
        this.uninstallMonitoring();
      }
    });
  }

  /**
   * Uninstall monitoring
   */
  private uninstallMonitoring(): void {
    this.api.monitoringUninstall(this.systemStatus.namespace).subscribe({
      next: () => {
        this.message.success('监控系统卸载成功');
        setTimeout(() => this.loadSystemStatus(), 2000);
      },
      error: (err: unknown) => {
        console.error('Uninstall error:', err);
        this.message.error('卸载失败：' + this.getErrorMessage(err));
      }
    });
  }

  /**
   * Retry installation
   */
  retryInstallation(): void {
    localStorage.removeItem('polardbx-monitor-install-job');
    this.router.navigate(['/operations/monitoring/enable-wizard']);
  }
}
