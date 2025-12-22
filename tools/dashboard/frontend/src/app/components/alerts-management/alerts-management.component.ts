import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder } from '@angular/forms';
import { FormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzMessageService } from 'ng-zorro-antd/message';
import { ApiService } from '../../services/api.service';

@Component({
  selector: 'app-alerts-management',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule, FormsModule, NzCardModule, NzTableModule, NzButtonModule, NzIconModule, NzFormModule, NzInputModule, NzSelectModule],
  template: `
    <div class="page-wrapper alerts-management">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="notification" class="page-icon"></i>
            告警管理
          </h2>
          <p>规则包 / 路由 / 静默</p>
        </div>
      </div>
      
      <nz-card>
        <div class="card-content">
          <div class="toolbar">
            <button nz-button nzType="primary" (click)="reload()"><i nz-icon nzType="reload"></i>刷新</button>
            <button nz-button nzType="default" (click)="createProfile()"><i nz-icon nzType="plus"></i>新建规则包</button>
          </div>

          <h3>规则包</h3>
          <nz-table #profileTable [nzData]="profiles" nzBordered nzSize="middle">
            <thead>
              <tr>
                <th>名称</th>
                <th nzWidth="120px">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let p of profileTable.data">
                <td>{{ p }}</td>
                <td>
                  <button nz-button nzType="link" nzSize="small" (click)="viewProfile(p)">
                    <i nz-icon nzType="eye"></i>
                  </button>
                  <button nz-button nzType="link" nzSize="small" (click)="editProfile(p)">
                    <i nz-icon nzType="edit"></i>
                  </button>
                  <button nz-button nzType="link" nzSize="small" nzDanger (click)="deleteProfile(p)">
                    <i nz-icon nzType="delete"></i>
                  </button>
                </td>
              </tr>
            </tbody>
          </nz-table>

          <h3 style="margin-top: 20px;">路由</h3>
          <div class="routes-block">
            <div class="routes-toolbar">
              <button nz-button nzType="primary" (click)="loadRoutes()">
                <i nz-icon nzType="download"></i> 加载
              </button>
              <button nz-button nzType="default" (click)="saveRoutes()" [nzLoading]="savingRoutes">
                <i nz-icon nzType="save"></i> 保存
              </button>
            </div>
            <div class="routes-form">
              <nz-form-item>
                <nz-form-label>Alertmanager URL（可选，用于静默与测试直连）</nz-form-label>
                <nz-form-control>
                  <input nz-input [(ngModel)]="routeAlertmanagerUrl" placeholder="http://alertmanager:9093">
                </nz-form-control>
              </nz-form-item>
              <nz-form-item>
                <nz-form-label>Route 配置（YAML）</nz-form-label>
                <nz-form-control>
                  <textarea nz-input rows="10" [(ngModel)]="routeContent" placeholder="# routes: ..."></textarea>
                </nz-form-control>
              </nz-form-item>
            </div>
          </div>

          <h3>静默</h3>
          <nz-table #silenceTable [nzData]="silences" nzBordered nzSize="middle">
            <thead>
              <tr>
                <th>ID</th>
                <th>匹配</th>
                <th nzWidth="100px">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr *ngFor="let s of silenceTable.data">
                <td>{{ s.id }}</td>
                <td>{{ s.matchers?.length || 0 }}</td>
                <td>
                  <button nz-button nzType="link" nzSize="small" nzDanger (click)="deleteSilence(s.id)">
                    <i nz-icon nzType="delete"></i>
                  </button>
                </td>
              </tr>
            </tbody>
          </nz-table>
        </div>
      </nz-card>
    </div>
  `,
  styles: [`
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background: transparent;
    }

    .page-header {
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .title-block h2 {
      margin: 0 0 8px;
      font-size: 22px;
      font-weight: 600;
      color: #1f1f1f;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .title-block p {
      margin: 0;
      color: #595959;
      line-height: 1.6;
    }

    .card-content { 
      padding: 24px; 
    }
    
    .toolbar { 
      margin: 0 0 16px 0; 
      display: flex; 
      gap: 8px; 
    }
    
    nz-table { 
      margin: 16px 0; 
    }
    
    .routes-block { 
      margin: 16px 0; 
    }
    
    .routes-toolbar { 
      margin: 16px 0; 
      display: flex; 
      gap: 8px; 
    }
    
    .routes-form { 
      margin: 16px 0; 
    }
    
    h3 {
      color: rgba(0, 0, 0, 0.85);
      font-weight: 600;
      margin: 24px 0 16px 0;
    }
    
    nz-form-item {
      margin-bottom: 16px;
    }
  `]
})
export class AlertsManagementComponent {
  profiles: string[] = [];
  silences: any[] = [];
  silenceCols = ['id','matchers','actions'];

  // Routes state
  routeContent = '';
  routeAlertmanagerUrl = '';
  savingRoutes = false;

  constructor(private api: ApiService, private message: NzMessageService, private fb: FormBuilder) {}

  reload() {
    this.api.listAlertProfiles().subscribe(r => this.profiles = r?.names || []);
    this.api.listSilences().subscribe(r => this.silences = r || []);
    this.loadRoutes();
  }

  createProfile() {
    const name = prompt('输入规则包名称');
    if (!name) return;
    const content = 'groups: []\n';
    this.api.createAlertProfile(name, content).subscribe(() => {
      this.message.success('创建成功');
      this.reload();
    });
  }

  viewProfile(name: string) {
    this.api.getAlertProfile(name).subscribe(p => {
      alert(`规则包: ${p.name}\n\n${p.content?.slice(0, 500)}`);
    });
  }

  editProfile(name: string) {
    this.api.getAlertProfile(name).subscribe(p => {
      const content = prompt(`编辑规则包 ${name}`, p.content || '');
      if (content == null) return;
      this.api.updateAlertProfile(name, content).subscribe(() => {
        this.message.success('已更新');
        this.reload();
      });
    });
  }

  deleteProfile(name: string) {
    if (!confirm(`删除规则包 ${name} ?`)) return;
    this.api.deleteAlertProfile(name).subscribe(() => {
      this.message.success('已删除');
      this.reload();
    });
  }

  deleteSilence(id: string) {
    if (!confirm(`删除静默 ${id} ?`)) return;
    this.api.deleteSilence(id).subscribe(() => {
      this.message.success('已删除');
      this.reload();
    });
  }

  // ===== Routes =====
  loadRoutes(): void {
    this.api.getAlertRoutes().subscribe({
      next: r => {
        this.routeContent = r?.content || '';
        this.routeAlertmanagerUrl = r?.alertmanagerUrl || '';
      },
      error: _ => {
        this.routeContent = '';
      }
    });
  }

  saveRoutes(): void {
    this.savingRoutes = true;
    this.api.putAlertRoutes({ content: this.routeContent || '', alertmanagerUrl: this.routeAlertmanagerUrl || undefined }).subscribe({
      next: _ => this.message.success('路由已保存'),
      error: e => this.message.error('保存失败: ' + (e?.error?.message || e?.message || '未知错误')),
      complete: () => this.savingRoutes = false
    });
  }
}