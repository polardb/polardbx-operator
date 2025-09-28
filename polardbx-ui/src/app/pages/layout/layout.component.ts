import { Component, OnInit } from '@angular/core';
import { RouterModule, Router } from '@angular/router';
import { CommonModule } from '@angular/common';

import { NzIconModule, NZ_ICONS, NzIconService } from 'ng-zorro-antd/icon';
import { NzMenuModule } from 'ng-zorro-antd/menu';
import { NzLayoutModule } from 'ng-zorro-antd/layout';
import {
  MenuFoldOutline,
  MenuUnfoldOutline,
  AppstoreOutline,
  CloudUploadOutline,
  UndoOutline,
  DatabaseOutline,
  SettingOutline,
  SaveOutline,
  CalendarOutline,
  FileTextOutline,
  HddOutline,
  DeploymentUnitOutline,
  DashboardOutline,
  SlidersOutline,
  ProfileOutline,
  FileSearchOutline,
  HistoryOutline,
  BellOutline,
  ToolOutline,
  AlertOutline,
  NotificationOutline,
  RollbackOutline,
  PieChartOutline,
  FileDoneOutline,
  InboxOutline,
  BuildOutline,
  ClusterOutline,
  CheckCircleOutline,
  TeamOutline,
  ContainerOutline,
  ControlOutline,
  ApiOutline,
  BookOutline
} from '@ant-design/icons-angular/icons';
import { MatSelectModule } from '@angular/material/select';
import { MatFormFieldModule } from '@angular/material/form-field';
import { FormsModule } from '@angular/forms';
import { NamespaceService } from '../../services/namespace.service';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzMessageModule, NzMessageService } from 'ng-zorro-antd/message';
import { LoginDialogComponent } from '../../components/login-dialog/login-dialog.component';
import { AuthService } from '../../services/auth.service';

const icons = [
  MenuFoldOutline,
  MenuUnfoldOutline,
  AppstoreOutline,
  CloudUploadOutline,
  UndoOutline,
  DatabaseOutline,
  SettingOutline,
  SaveOutline,
  CalendarOutline,
  FileTextOutline,
  HddOutline,
  DeploymentUnitOutline,
  DashboardOutline,
  SlidersOutline,
  ProfileOutline,
  FileSearchOutline,
  HistoryOutline,
  BellOutline,
  ToolOutline,
  AlertOutline,
  NotificationOutline,
  RollbackOutline,
  PieChartOutline,
  FileDoneOutline,
  InboxOutline,
  BuildOutline,
  ClusterOutline,
  CheckCircleOutline,
  TeamOutline,
  ContainerOutline,
  ControlOutline,
  ApiOutline,
  BookOutline
];

@Component({
  selector: 'app-layout',
  standalone: true,
  imports: [
    CommonModule, 
    RouterModule, 
    NzIconModule, 
    NzMenuModule, 
    NzLayoutModule, 
    MatSelectModule, 
    MatFormFieldModule, 
    FormsModule, 
    MatDialogModule, 
    MatIconModule, 
    NzButtonModule, 
    NzSelectModule, 
    NzDividerModule,
    NzModalModule,
    NzMessageModule
  ],
  templateUrl: './layout.component.html',
  styleUrls: ['./layout.component.scss'],
  providers: [{ provide: NZ_ICONS, useValue: icons }]
})
export class LayoutComponent implements OnInit {
  isCollapsed = false;
  namespaces: string[] = [];
  activeNamespace: string | null = null;
  isAuthenticated = false;

  constructor(
    private iconService: NzIconService, 
    private ns: NamespaceService, 
    private dialog: MatDialog, 
    private auth: AuthService,
    private router: Router,
    private message: NzMessageService,
    private modal: NzModalService
  ) {
    // 确保运行时已注册，避免仅依赖providers导致的加载顺序问题
    this.iconService.addIcon(...icons);
  }

  async ngOnInit() {
    await this.ns.init();
    this.ns.namespaces$.subscribe(list => this.namespaces = list || []);
    this.ns.activeNamespace$.subscribe(ns => this.activeNamespace = ns);
    await this.auth.init();
    
    // 监听认证状态
    this.updateAuthStatus();
  }
  
  private updateAuthStatus(): void {
    this.isAuthenticated = this.auth.isAuthenticated();
  }

  async openLogin() {
    const s = await this.auth.session$.toPromise();
    if (!s?.enabled) return; // JWT 未启用
    this.dialog.open(LoginDialogComponent, { width: '360px' });
  }

  logout(): void {
    this.modal.confirm({
      nzTitle: '确认登出',
      nzContent: '确定要断开与 Kubernetes 集群的连接吗？',
      nzOkText: '确定',
      nzCancelText: '取消',
      nzOnOk: () => {
        // 清除认证信息
        this.auth.clearKubeconfig();
        this.auth.logout();
        
        // 显示成功消息
        this.message.success('已成功登出');
        
        // 跳转到连接页面
        this.router.navigate(['/connect']);
      }
    });
  }

  reconnect(): void {
    this.modal.confirm({
      nzTitle: '重新连接',
      nzContent: '将清除当前连接信息并跳转到连接页面，确认继续吗？',
      nzOkText: '确定',
      nzCancelText: '取消',
      nzOnOk: () => {
        // 清除认证信息
        this.auth.clearKubeconfig();
        
        // 显示提示消息
        this.message.info('请重新配置 kubeconfig 连接信息');
        
        // 跳转到连接页面
        this.router.navigate(['/connect']);
      }
    });
  }

  onChangeNamespace(value: string) {
    this.ns.setActive(value);
  }
} 