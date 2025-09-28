import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatTableModule } from '@angular/material/table';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { MatChipsModule } from '@angular/material/chips';

import { ApiService } from '../../services/api.service';
import { Pod } from '../../models/pod.model';
import { PodRoleDetector, PodRoleInfo } from '../../utils/pod-role-detector';

@Component({
  selector: 'app-nodes',
  standalone: true,
  imports: [
    CommonModule, FormsModule,
    MatCardModule, MatButtonModule, MatIconModule, MatTableModule,
    MatFormFieldModule, MatInputModule, MatSelectModule, MatTooltipModule,
    MatDialogModule, MatChipsModule
  ],
  template: `
  <div class="page">
    <div class="header">
      <h2>节点</h2>
      <div class="actions">
        <mat-form-field appearance="outline" class="w-160">
          <mat-label>命名空间</mat-label>
          <input matInput [(ngModel)]="namespace" placeholder="default"/>
        </mat-form-field>
        <mat-form-field appearance="outline" class="w-200">
          <mat-label>搜索名称/IP</mat-label>
          <input matInput [(ngModel)]="keyword"/>
        </mat-form-field>
        <mat-form-field appearance="outline" class="w-160">
          <mat-label>角色</mat-label>
          <mat-select [(ngModel)]="roleFilter">
            <mat-option value="">全部</mat-option>
            <mat-option value="CN">CN (计算节点)</mat-option>
            <mat-option value="DN">DN (数据节点)</mat-option>
            <mat-option value="GMS">GMS (元服务)</mat-option>
            <mat-option value="CDC">CDC (数据捕获)</mat-option>
            <mat-option value="MinIO">MinIO (存储)</mat-option>
            <mat-option value="SFTP">SFTP (文件服务)</mat-option>
            <mat-option value="HPFS">HPFS (文件服务)</mat-option>
            <mat-option value="Unknown">未知</mat-option>
          </mat-select>
        </mat-form-field>
        <button mat-stroked-button (click)="load()"><mat-icon>refresh</mat-icon> 刷新</button>
      </div>
    </div>

    <div class="summary">
      <mat-card class="sum-card" *ngFor="let s of summary">
        <div class="title">{{ s.role }}</div>
        <div class="num">{{ s.ready }}/{{ s.total }}</div>
      </mat-card>
    </div>

    <div class="table">
      <table mat-table [dataSource]="filtered" class="mat-elevation-z1">
        <ng-container matColumnDef="name">
          <th mat-header-cell *matHeaderCellDef>名称</th>
          <td mat-cell *matCellDef="let n">{{ n.name }}</td>
        </ng-container>
        <ng-container matColumnDef="role">
          <th mat-header-cell *matHeaderCellDef>角色</th>
          <td mat-cell *matCellDef="let n"><mat-chip [color]="roleColor(n.role)">{{ n.role }}</mat-chip></td>
        </ng-container>
        <ng-container matColumnDef="status">
          <th mat-header-cell *matHeaderCellDef>状态</th>
          <td mat-cell *matCellDef="let n"><mat-chip [color]="statusColor(n.status)">{{ n.status }}</mat-chip></td>
        </ng-container>
        <ng-container matColumnDef="ip">
          <th mat-header-cell *matHeaderCellDef>IP</th>
          <td mat-cell *matCellDef="let n">{{ n.ip }}</td>
        </ng-container>
        <ng-container matColumnDef="actions">
          <th mat-header-cell *matHeaderCellDef>操作</th>
          <td mat-cell *matCellDef="let n">
            <button mat-icon-button matTooltip="查看详情" (click)="openDetail(n)"><mat-icon>info</mat-icon></button>
            <button mat-icon-button matTooltip="终端" (click)="openTerminal(n)"><mat-icon>computer</mat-icon></button>
            <button mat-icon-button matTooltip="执行命令" (click)="openExec(n)"><mat-icon>play_arrow</mat-icon></button>
          </td>
        </ng-container>
        <tr mat-header-row *matHeaderRowDef="cols"></tr>
        <tr mat-row *matRowDef="let row; columns: cols"></tr>
      </table>
    </div>
  </div>
  `,
  styles: [`
    .page { padding: 16px; }
    .header { display:flex; align-items:center; justify-content: space-between; }
    .actions { display:flex; gap:8px; align-items:center; }
    .w-160 { width:160px; }
    .w-200 { width:200px; }
    .summary { display:grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin: 12px 0; }
    .sum-card { padding: 12px; display:flex; justify-content: space-between; align-items: center; }
    .sum-card .title { color:#666; }
    .sum-card .num { font-size: 20px; font-weight: 600; }
    .table { border: 1px solid var(--pd-border); border-radius: var(--pd-radius); overflow: hidden; }
  `]
})
export class NodesComponent implements OnInit {
  private api = inject(ApiService);
  private dialog = inject(MatDialog);
  private router = inject(Router);

  namespace = 'default';
  keyword = '';
  roleFilter = '';

  all: Array<{ name: string; role: string; roleInfo: PodRoleInfo; status: string; ip: string; pod: Pod }>=[];
  filtered: typeof this.all = [];
  summary: Array<{ role: string; ready: number; total: number; category: string }>=[];
  cols = ['name','role','status','ip','actions'];

  ngOnInit(): void {
    this.load();
  }

  load(): void {
    // 改为按命名空间列出所有 Pod，避免 "*" 触发 400
    this.api.listPods(this.namespace).subscribe({
      next: (pods: Pod[]) => {
        const items: typeof this.all = [];
        const roleStats = new Map<string, { ready: number; total: number; category: string }>();
        
        for (const p of pods) {
          const name = p.metadata?.name || '';
          const roleInfo = PodRoleDetector.detectRole(p);
          const phase = p.status?.phase || '未知';
          const ip = p.status?.podIP || '';
          
          items.push({ 
            name, 
            role: roleInfo.role, 
            roleInfo,
            status: phase, 
            ip, 
            pod: p 
          });
          
          // 统计角色分布
          const key = roleInfo.role;
          if (!roleStats.has(key)) {
            roleStats.set(key, { 
              ready: 0, 
              total: 0, 
              category: roleInfo.category 
            });
          }
          const stats = roleStats.get(key)!;
          stats.total++;
          if (phase?.toLowerCase() === 'running') {
            stats.ready++;
          }
        }
        
        this.all = items;
        this.applyFilter();
        
        // 生成摘要，按类别和重要性排序
        this.summary = Array.from(roleStats.entries())
          .map(([role, stats]) => ({ 
            role, 
            ready: stats.ready, 
            total: stats.total,
            category: stats.category
          }))
          .sort((a, b) => {
            // 排序优先级：计算 > 存储 > 服务 > 监控 > 未知
            const priorityOrder = ['compute', 'storage', 'service', 'monitor', 'unknown'];
            const aPriority = priorityOrder.indexOf(a.category);
            const bPriority = priorityOrder.indexOf(b.category);
            if (aPriority !== bPriority) {
              return aPriority - bPriority;
            }
            // 同类别按角色名称排序
            return a.role.localeCompare(b.role);
          });
      },
      error: () => {
        this.all = []; this.filtered = []; this.summary = [];
      }
    });
  }

  applyFilter(): void {
    const kw = (this.keyword||'').toLowerCase();
    this.filtered = this.all.filter(n =>
      (!this.roleFilter || n.role === this.roleFilter) &&
      (!kw || n.name.toLowerCase().includes(kw) || n.ip.toLowerCase().includes(kw))
    );
  }

  roleColor(role: string): 'primary'|'accent'|'warn'|'basic' {
    // 为了兼容性，先尝试从当前项目中找到roleInfo
    const item = this.all.find(n => n.role === role);
    if (item?.roleInfo) {
      return PodRoleDetector.getRoleColor(item.roleInfo) as any;
    }
    // 回退到简单映射
    switch(role){ 
      case 'CN': return 'primary'; 
      case 'DN': return 'accent'; 
      case 'GMS': return 'warn'; 
      case 'CDC': return 'basic';
      case 'MinIO': return 'accent';
      case 'SFTP': case 'HPFS': return 'basic';
      default: return 'basic'; 
    }
  }
  statusColor(status: string): 'primary'|'accent'|'warn'|'basic' {
    const result = PodRoleDetector.getStatusColor(status);
    return result as 'primary'|'accent'|'warn'|'basic';
  }

  openDetail(n: {pod: Pod}): void {
    const podName = n.pod?.metadata?.name || '';
    if (!podName) return;
    this.router.navigate(['/operations','nodes', this.namespace, podName]);
  }
  openTerminal(n: {pod: Pod}): void {
    import('../../components/webshell-dialog/webshell-dialog.component').then(m => {
      const containers = (n.pod.spec?.containers||[]).map(c=>c.name);
      this.dialog.open(m.WebShellDialogComponent, { width:'900px', height:'600px', data: { namespace: this.namespace, pod: n.pod.metadata!.name, container: containers[0]||'', containers } });
    });
  }
  openExec(n: {pod: Pod}): void {
    import('../../components/exec-command-dialog/exec-command-dialog.component').then(m => {
      const containers = (n.pod.spec?.containers||[]).map(c=>c.name);
      this.dialog.open(m.ExecCommandDialogComponent, { width:'700px', data: { namespace: this.namespace, pod: n.pod.metadata!.name, containers, defaultContainer: containers[0]||'' } });
    });
  }
}

