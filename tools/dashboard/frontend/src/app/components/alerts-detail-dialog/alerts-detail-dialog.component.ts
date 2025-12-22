import { Component, Inject, OnInit } from '@angular/core';
import { CommonModule, DatePipe } from '@angular/common';
import { MatDialogRef, MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatCardModule } from '@angular/material/card';
import { MatChipsModule } from '@angular/material/chips';
import { MatTableModule, MatTableDataSource } from '@angular/material/table';
import { MatTabsModule } from '@angular/material/tabs';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatSelectModule } from '@angular/material/select';
import { MatNativeDateModule } from '@angular/material/core';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { FormsModule } from '@angular/forms';

interface AlertDetail {
  id: string;
  name: string;
  severity: 'critical' | 'warning' | 'info';
  status: 'firing' | 'pending' | 'resolved';
  message: string;
  source: string;
  startTime: Date;
  endTime?: Date;
  labels: Record<string, string>;
  annotations: Record<string, string>;
}

interface AlertsDialogData {
  cluster: any;
  alerts: {
    critical: number;
    warning: number;
    info: number;
    total: number;
    source: string;
  };
  namespace: string;
  clusterName: string;
}

@Component({
  selector: 'app-alerts-detail-dialog',
  standalone: true,
  imports: [
    CommonModule,
    DatePipe,
    FormsModule,
    MatDialogModule,
    MatButtonModule,
    MatIconModule,
    MatCardModule,
    MatChipsModule,
    MatTableModule,
    MatTabsModule,
    MatInputModule,
    MatFormFieldModule,
    MatSelectModule,
    MatNativeDateModule,
    MatProgressSpinnerModule
  ],
  template: `
    <div class="alerts-dialog">
      <div class="dialog-header">
        <h2 mat-dialog-title>
          <mat-icon>warning</mat-icon>
          集群告警详情 - {{ data.clusterName }}
        </h2>
        <button mat-icon-button mat-dialog-close>
          <mat-icon>close</mat-icon>
        </button>
      </div>

      <div mat-dialog-content class="dialog-content">
        
        <!-- 告警摘要 -->
        <mat-card class="summary-card">
          <mat-card-header>
            <mat-card-title>告警摘要</mat-card-title>
          </mat-card-header>
          <mat-card-content>
            <div class="summary-grid">
              <div class="summary-item critical">
                <mat-icon>error</mat-icon>
                <div class="count">{{ data.alerts.critical }}</div>
                <div class="label">严重</div>
              </div>
              <div class="summary-item warning">
                <mat-icon>warning</mat-icon>
                <div class="count">{{ data.alerts.warning }}</div>
                <div class="label">警告</div>
              </div>
              <div class="summary-item info">
                <mat-icon>info</mat-icon>
                <div class="count">{{ data.alerts.info }}</div>
                <div class="label">信息</div>
              </div>
              <div class="summary-item total">
                <mat-icon>assessment</mat-icon>
                <div class="count">{{ data.alerts.total }}</div>
                <div class="label">总计</div>
              </div>
            </div>
            <div class="alert-source">
              <mat-icon>source</mat-icon>
              <span>数据源: {{ data.alerts.source || '系统内置' }}</span>
            </div>
          </mat-card-content>
        </mat-card>

        <!-- 告警列表 -->
        <mat-card class="alerts-list-card">
          <mat-card-header>
            <mat-card-title>活跃告警</mat-card-title>
            <div class="header-actions">
              <mat-form-field appearance="outline" class="filter-field">
                <mat-label>筛选严重级别</mat-label>
                <mat-select [(value)]="selectedSeverity" (selectionChange)="filterAlerts()">
                  <mat-option value="">全部</mat-option>
                  <mat-option value="critical">严重</mat-option>
                  <mat-option value="warning">警告</mat-option>
                  <mat-option value="info">信息</mat-option>
                </mat-select>
              </mat-form-field>
              <button mat-icon-button matTooltip="刷新告警" (click)="refreshAlerts()">
                <mat-icon>refresh</mat-icon>
              </button>
            </div>
          </mat-card-header>
          <mat-card-content>
            
            <!-- 加载状态 -->
            <div *ngIf="isLoading" class="loading-container">
              <mat-progress-spinner mode="indeterminate" diameter="40"></mat-progress-spinner>
              <p>正在加载告警数据...</p>
            </div>

            <!-- 无告警状态 -->
            <div *ngIf="!isLoading && filteredAlerts.length === 0" class="no-alerts">
              <mat-icon>check_circle</mat-icon>
              <h3>暂无告警</h3>
              <p>当前集群运行正常，没有活跃的告警信息。</p>
            </div>

            <!-- 告警表格 -->
            <div *ngIf="!isLoading && filteredAlerts.length > 0" class="alerts-table">
              <table mat-table [dataSource]="alertDataSource" class="full-width-table">
                
                <!-- 严重级别列 -->
                <ng-container matColumnDef="severity">
                  <th mat-header-cell *matHeaderCellDef>级别</th>
                  <td mat-cell *matCellDef="let alert">
                    <mat-chip [class]="'severity-' + alert.severity">
                      <mat-icon>{{ getSeverityIcon(alert.severity) }}</mat-icon>
                      {{ getSeverityLabel(alert.severity) }}
                    </mat-chip>
                  </td>
                </ng-container>

                <!-- 告警名称列 -->
                <ng-container matColumnDef="name">
                  <th mat-header-cell *matHeaderCellDef>告警名称</th>
                  <td mat-cell *matCellDef="let alert">
                    <div class="alert-name">
                      <strong>{{ alert.name }}</strong>
                      <div class="alert-message">{{ alert.message }}</div>
                    </div>
                  </td>
                </ng-container>

                <!-- 来源列 -->
                <ng-container matColumnDef="source">
                  <th mat-header-cell *matHeaderCellDef>来源</th>
                  <td mat-cell *matCellDef="let alert">{{ alert.source }}</td>
                </ng-container>

                <!-- 开始时间列 -->
                <ng-container matColumnDef="startTime">
                  <th mat-header-cell *matHeaderCellDef>开始时间</th>
                  <td mat-cell *matCellDef="let alert">{{ alert.startTime | date:'yyyy-MM-dd HH:mm:ss' }}</td>
                </ng-container>

                <!-- 状态列 -->
                <ng-container matColumnDef="status">
                  <th mat-header-cell *matHeaderCellDef>状态</th>
                  <td mat-cell *matCellDef="let alert">
                    <mat-chip [class]="'status-' + alert.status">{{ getStatusLabel(alert.status) }}</mat-chip>
                  </td>
                </ng-container>

                <!-- 操作列 -->
                <ng-container matColumnDef="actions">
                  <th mat-header-cell *matHeaderCellDef>操作</th>
                  <td mat-cell *matCellDef="let alert">
                    <button mat-icon-button matTooltip="查看详情" (click)="viewAlertDetail(alert)">
                      <mat-icon>visibility</mat-icon>
                    </button>
                    <button mat-icon-button matTooltip="静音" (click)="silenceAlert(alert)" [disabled]="alert.status === 'resolved'">
                      <mat-icon>volume_off</mat-icon>
                    </button>
                  </td>
                </ng-container>

                <tr mat-header-row *matHeaderRowDef="displayedColumns"></tr>
                <tr mat-row *matRowDef="let row; columns: displayedColumns;" [class.resolved-row]="row.status === 'resolved'"></tr>
              </table>
            </div>
          </mat-card-content>
        </mat-card>
      </div>

      <div mat-dialog-actions class="dialog-actions">
        <button mat-button (click)="exportAlerts()">
          <mat-icon>download</mat-icon>
          导出告警
        </button>
        <button mat-button (click)="openAlertManager()" *ngIf="data.alerts.source !== 'none'">
          <mat-icon>open_in_new</mat-icon>
          打开 AlertManager
        </button>
        <button mat-button mat-dialog-close>关闭</button>
        <button mat-raised-button color="primary" (click)="closeAndRefresh()">
          <mat-icon>refresh</mat-icon>
          刷新并关闭
        </button>
      </div>
    </div>
  `,
  styleUrls: ['./alerts-detail-dialog.component.scss']
})
export class AlertsDetailDialogComponent implements OnInit {
  
  // 模拟告警数据模板
  private getMockAlerts(): AlertDetail[] {
    return [
      {
        id: '1',
        name: 'PolarDBX-CN-HighCPU',
        severity: 'warning',
        status: 'firing',
        message: 'CN节点CPU使用率超过80%',
        source: 'Prometheus',
        startTime: new Date(Date.now() - 3600000), // 1小时前
        labels: { cluster: this.data.clusterName, instance: 'cn-0', job: 'polardbx-cn' },
        annotations: { description: 'CN节点CPU使用率持续超过80%，可能影响查询性能' }
      },
      {
        id: '2',
        name: 'PolarDBX-DN-DiskSpaceHigh',
        severity: 'critical',
        status: 'firing',
        message: 'DN节点磁盘空间不足',
        source: 'Prometheus',
        startTime: new Date(Date.now() - 1800000), // 30分钟前
        labels: { cluster: this.data.clusterName, instance: 'dn-0', job: 'polardbx-dn' },
        annotations: { description: 'DN节点磁盘使用率超过90%，需要立即清理或扩容' }
      },
      {
        id: '3',
        name: 'PolarDBX-Connection-Count-High',
        severity: 'info',
        status: 'pending',
        message: '连接数接近上限',
        source: 'System',
        startTime: new Date(Date.now() - 900000), // 15分钟前
        labels: { cluster: this.data.clusterName, type: 'connection' },
        annotations: { description: '当前连接数达到配置上限的80%' }
      }
    ];
  }

  alertDataSource = new MatTableDataSource<AlertDetail>([]);
  filteredAlerts: AlertDetail[] = [];
  displayedColumns: string[] = ['severity', 'name', 'source', 'startTime', 'status', 'actions'];
  selectedSeverity = '';
  isLoading = false;

  constructor(
    public dialogRef: MatDialogRef<AlertsDetailDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: AlertsDialogData
  ) {}

  ngOnInit() {
    this.loadAlerts();
  }

  loadAlerts() {
    this.isLoading = true;
    
    // 模拟API调用延迟
    setTimeout(() => {
      // 根据摘要数据生成对应数量的告警
      const alerts: AlertDetail[] = [];
      const mockAlerts = this.getMockAlerts();
      
      // 添加严重告警
      for (let i = 0; i < this.data.alerts.critical; i++) {
        alerts.push({
          ...mockAlerts[1], // 使用磁盘空间告警模板
          id: `critical-${i}`,
          name: `Critical-Alert-${i + 1}`,
        });
      }
      
      // 添加警告告警
      for (let i = 0; i < this.data.alerts.warning; i++) {
        alerts.push({
          ...mockAlerts[0], // 使用CPU告警模板
          id: `warning-${i}`,
          name: `Warning-Alert-${i + 1}`,
        });
      }
      
      // 添加信息告警
      for (let i = 0; i < this.data.alerts.info; i++) {
        alerts.push({
          ...mockAlerts[2], // 使用连接数告警模板
          id: `info-${i}`,
          name: `Info-Alert-${i + 1}`,
        });
      }

      this.filteredAlerts = alerts;
      this.alertDataSource.data = this.filteredAlerts;
      this.isLoading = false;
    }, 1000);
  }

  filterAlerts() {
    if (!this.selectedSeverity) {
      this.filteredAlerts = [...this.alertDataSource.data];
    } else {
      this.filteredAlerts = this.alertDataSource.data.filter(alert => alert.severity === this.selectedSeverity);
    }
    this.alertDataSource.data = this.filteredAlerts;
  }

  refreshAlerts() {
    this.loadAlerts();
  }

  getSeverityIcon(severity: string): string {
    switch (severity) {
      case 'critical': return 'error';
      case 'warning': return 'warning';
      case 'info': return 'info';
      default: return 'help';
    }
  }

  getSeverityLabel(severity: string): string {
    switch (severity) {
      case 'critical': return '严重';
      case 'warning': return '警告';
      case 'info': return '信息';
      default: return '未知';
    }
  }

  getStatusLabel(status: string): string {
    switch (status) {
      case 'firing': return '触发中';
      case 'pending': return '等待中';
      case 'resolved': return '已解决';
      default: return '未知';
    }
  }

  viewAlertDetail(alert: AlertDetail) {
    // 显示告警详细信息
    const detailMessage = `
告警详情：

名称: ${alert.name}
级别: ${this.getSeverityLabel(alert.severity)}
状态: ${this.getStatusLabel(alert.status)}
消息: ${alert.message}
来源: ${alert.source}
开始时间: ${alert.startTime.toLocaleString()}

标签:
${Object.entries(alert.labels).map(([key, value]) => `  ${key}: ${value}`).join('\n')}

注解:
${Object.entries(alert.annotations).map(([key, value]) => `  ${key}: ${value}`).join('\n')}
    `;
    
    window.alert(detailMessage);
  }

  silenceAlert(alert: AlertDetail) {
    if (confirm(`确定要静音告警 "${alert.name}" 吗？`)) {
      // 这里应该调用API来静音告警
      alert.status = 'resolved';
      console.log('告警已静音:', alert.name);
    }
  }

  exportAlerts() {
    const data = this.filteredAlerts.map(alert => ({
      '告警名称': alert.name,
      '严重级别': this.getSeverityLabel(alert.severity),
      '状态': this.getStatusLabel(alert.status),
      '消息': alert.message,
      '来源': alert.source,
      '开始时间': alert.startTime.toISOString()
    }));

    const csv = this.convertToCSV(data);
    this.downloadCSV(csv, `${this.data.clusterName}-alerts.csv`);
  }

  openAlertManager() {
    const alertManagerUrl = localStorage.getItem('alertmanager') || 'http://localhost:9093';
    window.open(alertManagerUrl, '_blank');
  }

  closeAndRefresh() {
    this.dialogRef.close({ refreshAlerts: true });
  }

  private convertToCSV(data: any[]): string {
    if (data.length === 0) return '';
    
    const headers = Object.keys(data[0]);
    const csvRows = [];
    
    // 添加表头
    csvRows.push(headers.join(','));
    
    // 添加数据行
    for (const row of data) {
      const values = headers.map(header => {
        const value = row[header];
        return `"${value}"`;
      });
      csvRows.push(values.join(','));
    }
    
    return csvRows.join('\n');
  }

  private downloadCSV(csv: string, filename: string) {
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    link.click();
    window.URL.revokeObjectURL(url);
  }
}