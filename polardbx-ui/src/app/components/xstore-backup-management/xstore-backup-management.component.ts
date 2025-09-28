import { Component, inject, OnInit, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormBuilder, FormGroup, Validators, ReactiveFormsModule } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzProgressModule } from 'ng-zorro-antd/progress';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzMessageModule } from 'ng-zorro-antd/message';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzPopconfirmModule } from 'ng-zorro-antd/popconfirm';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzCollapseModule } from 'ng-zorro-antd/collapse';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { XStoreBackup, XStoreBackupWithStatus, CreateXStoreBackupRequest } from '../../models/xstore-backup.model';
import { XStoreBackupBinlog, CreateXStoreBackupBinlogRequest } from '../../models/xstore-backup-binlog.model';
import { XStore } from '../../models/xstore.model';
import { debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';
import { NamespaceService } from '../../services/namespace.service';
import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'app-xstore-backup-management',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzButtonModule,
    NzIconModule,
    NzTagModule,
    NzProgressModule,
    NzTableModule,
    NzToolTipModule,
    NzGridModule,
    NzFormModule,
    NzInputModule,
    NzSelectModule,
    NzCheckboxModule,
    NzDividerModule,
    NzInputNumberModule,
    NzAlertModule,
    NzTabsModule,
    NzModalModule,
    NzMessageModule,
    NzSpinModule,
    NzPopconfirmModule,
    NzSwitchModule,
    NzCollapseModule,
    NzEmptyModule,
  ],
  template: `
    <div class="xstore-backup-management">
      <!-- 页面头部 -->
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="hdd" class="page-icon"></i>
            XStore 备份管理
          </h1>
          <p class="page-description">管理 XStore 存储备份任务与增量日志备份配置</p>
        </div>
      </div>

      <div class="page-content">
        <nz-tabset class="main-tabs" [nzTabPosition]="'top'" [(nzSelectedIndex)]="selectedTab">
          <nz-tab nzTitle="存储备份任务">
              <ng-template nz-tab>
                <div class="tab-content">
                  <nz-card 
                    class="list-card" 
                    nzTitle="存储备份配置" 
                    [nzExtra]="listExtra"
                    [nzLoading]="loadingService.isLoading(loadingKeys.XSTORE_BACKUP_LIST)">
                    <ng-template #listExtra>
                      <div class="extra-actions">
                        <button nz-button nzType="default" nzSize="small" (click)="refreshBackups()">
                          <i nz-icon nzType="reload"></i>
                          刷新
                        </button>
                        <button nz-button nzType="primary" nzSize="small" (click)="createNew()" 
                                nz-tooltip="创建存储级全量/增量备份任务">
                          <i nz-icon nzType="plus"></i>
                          新建存储备份
                        </button>
                        <button nz-button nzType="default" nzSize="small" (click)="switchToBinlogTab()"
                                nz-tooltip="跳转到增量日志备份配置页面">
                          <i nz-icon nzType="file-text"></i>
                          新建日志备份
                        </button>
                        <div class="view-toggle">
                          <button nz-button [nzType]="viewMode==='summary' ? 'primary' : 'default'" nzSize="small" (click)="onChangeViewMode('summary')">汇总</button>
                          <button nz-button [nzType]="viewMode==='detail' ? 'primary' : 'default'" nzSize="small" (click)="onChangeViewMode('detail')">明细</button>
                        </div>
                      </div>
                    </ng-template>
                    <div class="list-content">
                      <nz-table 
                        #xTable 
                        [nzData]="backups" 
                        [nzLoading]="loadingService.isLoading(loadingKeys.XSTORE_BACKUP_LIST)"
                        [nzPageSize]="10"
                        [nzShowPagination]="backups.length > 10"
                        [nzScroll]="{ x: '1300px' }">
                        <thead>
                          <tr>
                            <th nzWidth="180px">名称</th>
                            <th nzWidth="200px">XStore</th>
                            <th nzWidth="150px">类别</th>
                            <th nzWidth="100px">类型</th>
                            <th nzWidth="120px">状态</th>
                            <th nzWidth="120px">大小</th>
                            <th nzWidth="150px">开始时间</th>
                            <th nzWidth="120px">进度</th>
                            <th nzWidth="180px" nzAlign="center">操作</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr *ngFor="let b of xTable.data">
                            <td>
                              <div class="backup-name">
                                <i nz-icon 
                                   [nzType]="getBackupTypeIcon(b.spec.backupType || 'full')"
                                   [style.color]="getNzStatusColor(b.status?.phase)">
                                </i>
                                <span style="margin-left: 8px;">{{ b.metadata.name }}</span>
                              </div>
                            </td>
                            <td>
                              <div class="xstore-display">
                                <i nz-icon nzType="database" class="xstore-icon"></i>
                                <span class="xstore-name">{{ getXStoreName(b) }}</span>
                              </div>
                            </td>
                            <td>
                              <div class="category-display">
                                <nz-tag nzColor="geekblue" *ngIf="getCategoryDisplay(b) as cat" 
                                        nz-tooltip [nzTooltipTitle]="getCategoryTooltip(b)"
                                        class="category-tag">
                                  {{ cat }}
                                </nz-tag>
                              </div>
                            </td>
                            <td>{{ (b.spec.backupType || 'full') === 'full' ? '全量' : '增量' }}</td>
                            <td>
                              <nz-tag [nzColor]="getNzStatusColor(b.status?.phase)">
                                {{ b.displayStatus || (b.status?.phase || '未知') }}
                              </nz-tag>
                            </td>
                            <td>{{ b.displaySize || '-' }}</td>
                            <td>{{ formatDate(b.status?.startTime) }}</td>
                            <td>
                              <nz-progress [nzPercent]="getProgressPercentage(b)" nzSize="small"></nz-progress>
                            </td>
                            <td nzAlign="center">
                              <div class="action-buttons">
                                <button 
                                  nz-button 
                                  nzType="link" 
                                  nzSize="small"
                                  nz-tooltip="查看详情"
                                  (click)="viewBackup(b)">
                                  <i nz-icon nzType="eye"></i>
                                </button>
                                <button 
                                  nz-button 
                                  nzType="link" 
                                  nzSize="small"
                                  nz-tooltip="编辑备份"
                                  (click)="editBackup(b)">
                                  <i nz-icon nzType="edit"></i>
                                </button>
                                <button 
                                  nz-button 
                                  nzType="link" 
                                  nzDanger
                                  nzSize="small"
                                  nz-tooltip="删除备份"
                                  nz-popconfirm
                                  nzPopconfirmTitle="确定要删除这个备份吗？"
                                  (nzOnConfirm)="deleteBackup(b)">
                                  <i nz-icon nzType="delete"></i>
                                </button>
                                <button 
                                  nz-button 
                                  nzType="link" 
                                  nzDanger
                                  nzSize="small"
                                  nz-tooltip="强制删除（移除finalizers）"
                                  nz-popconfirm
                                  nzPopconfirmTitle="强制删除将直接移除 finalizers，确定继续吗？"
                                  (nzOnConfirm)="forceDeleteBackup(b)">
                                  <i nz-icon nzType="close"></i>
                                </button>
                              </div>
                            </td>
                          </tr>
                        </tbody>
                      </nz-table>

                      <div *ngIf="!loadingService.isLoading(loadingKeys.XSTORE_BACKUP_LIST) && backups.length === 0" class="empty-state">
                        <nz-empty 
                          nzNotFoundImage="simple" 
                          nzNotFoundContent="暂无存储备份配置">
                          <div nz-empty-footer>
                            <div style="display: flex; gap: 8px;">
                              <button nz-button nzType="primary" (click)="createNew()"
                                      nz-tooltip="创建存储级全量/增量备份任务">
                                <i nz-icon nzType="plus"></i>
                                新建存储备份
                              </button>
                              <button nz-button nzType="default" (click)="switchToBinlogTab()"
                                      nz-tooltip="跳转到增量日志备份配置页面">
                                <i nz-icon nzType="file-text"></i>
                                新建日志备份
                              </button>
                            </div>
                          </div>
                        </nz-empty>
                      </div>
                    </div>
                  </nz-card>
                </div>
              </ng-template>
            </nz-tab>
            
            <nz-tab nzTitle="日志备份配置">
              <ng-template nz-tab>
                <div class="tab-content">
                  <nz-card 
                    class="list-card" 
                    nzTitle="增量日志备份配置（标准版）" 
                    [nzExtra]="binlogListExtra"
                    [nzLoading]="loadingService.isLoading(loadingKeys.XSTORE_BINLOG_LIST)">
                    <ng-template #binlogListExtra>
                      <div class="extra-actions">
                        <button nz-button nzType="default" nzSize="small" (click)="refreshBinlogBackups()">
                          <i nz-icon nzType="reload"></i>
                          刷新
                        </button>
                        <button nz-button nzType="primary" nzSize="small" (click)="createNewBinlog()">
                          <i nz-icon nzType="plus"></i>
                          新建日志备份配置
                        </button>
                      </div>
                    </ng-template>
                    <div class="list-content">
                      <nz-table 
                        #binlogTable 
                        [nzData]="binlogBackups" 
                        [nzLoading]="loadingService.isLoading(loadingKeys.XSTORE_BINLOG_LIST)"
                        [nzPageSize]="10"
                        [nzShowPagination]="binlogBackups.length > 10"
                        [nzScroll]="{ x: '1200px' }">
                        <thead>
                          <tr>
                            <th nzWidth="180px">名称</th>
                            <th nzWidth="180px">XStore</th>
                            <th nzWidth="120px">状态</th>
                            <th nzWidth="120px">远程保留</th>
                            <th nzWidth="120px">本地保留</th>
                            <th nzWidth="100px">PITR</th>
                            <th nzWidth="120px">存储类型</th>
                            <th nzWidth="180px" nzAlign="center">操作</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr *ngFor="let bl of binlogTable.data">
                            <td>
                              <div class="backup-name">
                                <i nz-icon nzType="file-text" [style.color]="getNzStatusColor(bl.status?.phase)"></i>
                                <span style="margin-left: 8px;">{{ bl.metadata.name }}</span>
                              </div>
                            </td>
                            <td>
                              <div class="xstore-display">
                                <i nz-icon nzType="database" class="xstore-icon"></i>
                                <span class="xstore-name">{{ bl.spec.xstoreName }}</span>
                              </div>
                            </td>
                            <td>
                              <nz-tag [nzColor]="getNzStatusColor(bl.status?.phase)">
                                {{ bl.status?.phase || '未知' }}
                              </nz-tag>
                            </td>
                            <td>{{ bl.spec.remoteExpireLogHours || 168 }}h</td>
                            <td>{{ bl.spec.localExpireLogHours || 7 }}h</td>
                            <td>
                              <nz-tag [nzColor]="bl.spec.pointInTimeRecover ? 'green' : 'default'">
                                {{ bl.spec.pointInTimeRecover ? '启用' : '禁用' }}
                              </nz-tag>
                            </td>
                            <td>{{ bl.spec.storageProvider?.storageName || '-' }}</td>
                            <td nzAlign="center">
                              <div class="action-buttons">
                                <button 
                                  nz-button 
                                  nzType="link" 
                                  nzSize="small"
                                  nz-tooltip="查看详情"
                                  (click)="viewBinlogBackup(bl)">
                                  <i nz-icon nzType="eye"></i>
                                </button>
                                <button 
                                  nz-button 
                                  nzType="link" 
                                  nzSize="small"
                                  nz-tooltip="编辑配置"
                                  (click)="editBinlogBackup(bl)">
                                  <i nz-icon nzType="edit"></i>
                                </button>
                                <button 
                                  nz-button 
                                  nzType="link" 
                                  nzDanger
                                  nzSize="small"
                                  nz-tooltip="删除配置"
                                  nz-popconfirm
                                  nzPopconfirmTitle="确定要删除这个增量日志备份配置吗？"
                                  (nzOnConfirm)="deleteBinlogBackup(bl)">
                                  <i nz-icon nzType="delete"></i>
                                </button>
                              </div>
                            </td>
                          </tr>
                        </tbody>
                      </nz-table>

                      <div *ngIf="!loadingService.isLoading(loadingKeys.XSTORE_BINLOG_LIST) && binlogBackups.length === 0" class="empty-state">
                        <nz-empty 
                          nzNotFoundImage="simple" 
                          nzNotFoundContent="暂无增量日志备份配置">
                          <div nz-empty-footer>
                            <button nz-button nzType="primary" (click)="createNewBinlog()">
                              <i nz-icon nzType="plus"></i>
                              新建日志备份配置
                            </button>
                          </div>
                        </nz-empty>
                      </div>
                    </div>
                  </nz-card>
                </div>
              </ng-template>
            </nz-tab>
            
            <!-- Binlog Create/Edit Modal -->
            <nz-modal
              [(nzVisible)]="binlogModalVisible"
              [nzTitle]="binlogForm.get('name')?.value ? '编辑增量日志备份' : '创建增量日志备份'"
              [nzMaskClosable]="false"
              [nzWidth]="720"
              (nzOnCancel)="binlogModalVisible=false"
              (nzOnOk)="saveBinlogBackup()">
              <div *nzModalContent>
                <form nz-form [formGroup]="binlogForm" nzLayout="vertical">
                  <div nz-row [nzGutter]="16">
                    <div nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>名称</nz-form-label>
                        <nz-form-control nzErrorTip="请输入名称">
                          <input nz-input formControlName="name" placeholder="binlog-backup-name" />
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                    <div nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>命名空间</nz-form-label>
                        <nz-form-control nzErrorTip="请输入命名空间">
                          <input nz-input formControlName="namespace" placeholder="default" />
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                  </div>

                  <div nz-row [nzGutter]="16">
                    <div nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>XStore</nz-form-label>
                        <nz-form-control nzErrorTip="请选择 XStore">
                          <nz-select formControlName="xstoreName" nzPlaceHolder="选择 XStore" nzShowSearch>
                            <nz-option *ngFor="let x of availableXStores" [nzValue]="x.metadata.name" [nzLabel]="x.metadata.name"></nz-option>
                          </nz-select>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                    <div nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>校验码</nz-form-label>
                        <nz-form-control>
                          <nz-select formControlName="binlogChecksum">
                            <nz-option nzValue="CRC32" nzLabel="CRC32"></nz-option>
                            <nz-option nzValue="NONE" nzLabel="NONE"></nz-option>
                          </nz-select>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                  </div>

                  <div nz-row [nzGutter]="16">
                    <div nz-col [nzSpan]="8">
                      <nz-form-item>
                        <nz-form-label>远程保留(小时)</nz-form-label>
                        <nz-form-control>
                          <nz-input-number [nzMin]="1" [nzStep]="1" formControlName="remoteExpireLogHours"></nz-input-number>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                    <div nz-col [nzSpan]="8">
                      <nz-form-item>
                        <nz-form-label>本地保留(小时)</nz-form-label>
                        <nz-form-control>
                          <nz-input-number [nzMin]="1" [nzStep]="1" formControlName="localExpireLogHours"></nz-input-number>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                    <div nz-col [nzSpan]="8">
                      <nz-form-item>
                        <nz-form-label>本地最大文件数</nz-form-label>
                        <nz-form-control>
                          <nz-input-number [nzMin]="1" [nzStep]="1" formControlName="maxLocalBinlogCount"></nz-input-number>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                  </div>

                  <div nz-row [nzGutter]="16">
                    <div nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label>PITR</nz-form-label>
                        <nz-form-control>
                          <label nz-checkbox formControlName="pointInTimeRecover">启用</label>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                    <div nz-col [nzSpan]="12">
                      <nz-form-item>
                        <nz-form-label nzRequired>存储类型</nz-form-label>
                        <nz-form-control>
                          <nz-select formControlName="storageType">
                            <nz-option nzValue="oss" nzLabel="OSS"></nz-option>
                            <nz-option nzValue="s3" nzLabel="S3/MinIO"></nz-option>
                            <nz-option nzValue="sftp" nzLabel="SFTP"></nz-option>
                          </nz-select>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                  </div>

                  <div nz-row [nzGutter]="16">
                    <div nz-col [nzSpan]="24">
                      <nz-form-item>
                        <nz-form-label nzRequired>Sink 名称</nz-form-label>
                        <nz-form-control nzErrorTip="请输入或选择 sink 名称">
                          <nz-select formControlName="sinkName" nzShowSearch nzAllowClear nzPlaceHolder="选择 HPFS 配置中的 sink 名称">
                            <nz-option *ngFor="let s of filteredHpfsSinksForBinlog" [nzValue]="s.name" [nzLabel]="s.name"></nz-option>
                          </nz-select>
                        </nz-form-control>
                      </nz-form-item>
                    </div>
                  </div>
                </form>
              </div>
            </nz-modal>

            <nz-tab nzTitle="创建备份配置">
              <ng-template nz-tab>
                <div class="tab-content">
                  <nz-card class="form-card" nzTitle="{{ data.mode === 'edit' ? '编辑备份配置' : '创建新备份配置' }}">
                    <nz-collapse [nzBordered]="false" nzExpandIconPosition="end">
                      <nz-collapse-panel [nzActive]="true" nzHeader="基础配置" [nzDisabled]="false">
                        <form nz-form [formGroup]="backupForm" nzLayout="vertical">
                          <div nz-row [nzGutter]="16">
                            <div nz-col [nzSpan]="8">
                              <nz-form-item>
                                <nz-form-label nzRequired>备份名称</nz-form-label>
                                <nz-form-control nzErrorTip="请输入有效的备份名称">
                                  <input nz-input formControlName="name" placeholder="输入备份配置名称" />
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                            <div nz-col [nzSpan]="8">
                              <nz-form-item>
                                <nz-form-label nzRequired>命名空间</nz-form-label>
                                <nz-form-control nzErrorTip="请输入命名空间">
                                  <input nz-input formControlName="namespace" placeholder="default" />
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                            <div nz-col [nzSpan]="8">
                              <nz-form-item>
                                <nz-form-label nzRequired>目标 XStore</nz-form-label>
                                <nz-form-control nzErrorTip="请选择要备份的 XStore">
                                  <nz-select formControlName="xStoreName" nzPlaceHolder="选择要备份的 XStore 实例" nzShowSearch>
                                    <nz-option *ngFor="let x of availableXStores" [nzValue]="x.metadata.name" [nzLabel]="x.metadata.name">
                                      <i nz-icon nzType="database" style="margin-right: 8px;"></i>
                                      {{ x.metadata.name }}
                                      <nz-tag nzColor="blue" style="margin-left: 8px;">{{ x.status?.phase || '未知' }}</nz-tag>
                                    </nz-option>
                                  </nz-select>
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                          </div>

                          <div nz-row [nzGutter]="16">
                            <div nz-col [nzSpan]="12">
                              <nz-form-item>
                                <nz-form-label nzRequired>备份类型</nz-form-label>
                                <nz-form-control>
                                  <nz-select formControlName="backupType">
                                    <nz-option *ngFor="let t of backupTypes" [nzValue]="t.value" [nzLabel]="t.label">
                                      <i nz-icon [nzType]="getBackupTypeIcon(t.value)" style="margin-right: 8px;"></i>
                                      {{ t.label }}
                                    </nz-option>
                                  </nz-select>
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                            <div nz-col [nzSpan]="12">
                              <nz-form-item>
                                <nz-form-label>定时表达式（可选）</nz-form-label>
                                <nz-form-control>
                                  <nz-input-group nzPrefixIcon="calendar" nzSuffixIcon="question-circle">
                                    <input nz-input formControlName="schedule" placeholder="例如：0 2 * * * （每天凌晨2点执行）" />
                                  </nz-input-group>
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                          </div>

                          <div nz-row [nzGutter]="16">
                            <div nz-col [nzSpan]="12">
                              <nz-form-item>
                                <nz-form-control>
                                  <label nz-checkbox formControlName="compression">启用压缩</label>
                                  <div style="color: #666; font-size: 12px; margin-top: 4px;">
                                    压缩备份文件以节省存储空间，但会增加 CPU 使用量
                                  </div>
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                            <div nz-col [nzSpan]="12">
                              <nz-form-item>
                                <nz-form-control>
                                  <label nz-checkbox formControlName="enableEncryption">启用加密</label>
                                  <div style="color: #666; font-size: 12px; margin-top: 4px;">
                                    对备份文件进行加密保护，提高数据安全性
                                  </div>
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                          </div>
                        </form>
                      </nz-collapse-panel>

                      <nz-collapse-panel nzHeader="存储配置" [nzActive]="false">
                        <form nz-form [formGroup]="storageForm" nzLayout="vertical">
                          <div nz-row [nzGutter]="16">
                            <div nz-col [nzSpan]="12">
                              <nz-form-item>
                                <nz-form-label nzRequired>存储类型</nz-form-label>
                                <nz-form-control>
                                  <nz-select formControlName="storageType" nzPlaceHolder="选择存储类型">
                                    <nz-option *ngFor="let s of storageProviders" [nzValue]="s.value" [nzLabel]="s.label">
                                      <i nz-icon [nzType]="getStorageIcon(s.value)" style="margin-right: 8px;"></i>
                                      {{ s.label }}
                                    </nz-option>
                                  </nz-select>
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                            <div nz-col [nzSpan]="12">
                              <nz-form-item>
                                <nz-form-label nzRequired>Sink 名称</nz-form-label>
                                <nz-form-control>
                                  <nz-select formControlName="sinkName" nzPlaceHolder="选择 HPFS 中已配置的 sink 名称" nzShowSearch>
                                    <nz-option *ngFor="let s of filteredHpfsSinks"
                                               [nzValue]="s.name" [nzLabel]="s.name">
                                      <i nz-icon [nzType]="getStorageIcon(s.type)"></i>
                                      {{ s.name }}
                                      <span style="color:#999;margin-left:8px">{{ s.endpoint || s.host || '' }}</span>
                                    </nz-option>
                                  </nz-select>
                                  <div style="margin-top:6px; font-size:12px;">
                                    <ng-container [ngSwitch]="sinkStatus">
                                      <span *ngSwitchCase="'valid'" style="color:#52c41a">已校验：sink 存在</span>
                                      <span *ngSwitchCase="'invalid'" style="color:#ff4d4f">未找到该 sink，请检查 HPFS 配置</span>
                                      <span *ngSwitchCase="'checking'" style="color:#1890ff">正在校验...</span>
                                      <span *ngSwitchDefault style="color:#999">从 HPFS ConfigMap 中选择 sink</span>
                                    </ng-container>
                                  </div>
                                </nz-form-control>
                              </nz-form-item>
                            </div>
                          </div>

                          <div *ngIf="selectedStorageType === 'oss'" formGroupName="ossConfig">
                            <nz-divider nzText="阿里云 OSS 配置" nzOrientation="left"></nz-divider>
                            <nz-alert 
                              nzType="info" 
                              nzMessage="配置说明" 
                              nzDescription="该信息应配置在 polardbx-operator-system/polardbx-hpfs-config 的 config.yaml 中（前端不保存凭据）"
                              nzShowIcon
                              style="margin-bottom: 16px;">
                            </nz-alert>
                            <pre class="config-summary" [innerText]="exampleOssYaml"></pre>
                        <div class="form-row">
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Access Key ID</nz-form-label>
                            <nz-form-control nzHasFeedback>
                              <input nz-input formControlName="accessKeyId" placeholder="请输入 Access Key ID" />
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Access Key Secret</nz-form-label>
                            <nz-form-control nzHasFeedback>
                              <nz-input-group>
                                <input nz-input [type]="showSecret ? 'text' : 'password'" formControlName="accessKeySecret" placeholder="请输入 Access Key Secret" />
                              </nz-input-group>
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        </div>
                        <div class="form-row">
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Bucket</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="bucket" placeholder="存储桶名称" />
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Endpoint</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="endpoint" placeholder="例如：oss-cn-beijing.aliyuncs.com" />
                            </nz-form-control>
                          </nz-form-item>
                      </div>
                      </div>
                    </div>

                      <div *ngIf="selectedStorageType === 's3'" formGroupName="s3Config">
                      <nz-divider nzText="Amazon S3 配置" nzOrientation="left"></nz-divider>
                      <div class="help-text">在 HPFS ConfigMap 中配置 S3 sink，例如：</div>
                      <pre class="config-summary" [innerText]="exampleS3Yaml"></pre>
                        <div class="form-row">
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Access Key ID</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="accessKeyId" placeholder="请输入 Access Key ID" />
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Secret Access Key</nz-form-label>
                            <nz-form-control>
                              <nz-input-group>
                                <input nz-input [type]="showSecret ? 'text' : 'password'" formControlName="secretAccessKey" placeholder="请输入 Secret Access Key" />
                              </nz-input-group>
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        </div>
                        <div class="form-row">
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Bucket</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="bucket" placeholder="存储桶名称" />
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">Region</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="region" placeholder="例如：us-east-1" />
                            </nz-form-control>
                          </nz-form-item>
                      </div>
                      </div>
                    </div>

                      <div *ngIf="selectedStorageType === 'sftp'" formGroupName="sftpConfig">
                      <nz-divider nzText="SFTP 配置" nzOrientation="left"></nz-divider>
                      <div class="help-text">在 HPFS ConfigMap 中配置 SFTP sink，例如：</div>
                      <pre class="config-summary" [innerText]="exampleSftpYaml"></pre>
                        <div class="form-row">
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">主机地址</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="host" placeholder="SFTP 服务器地址" />
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">端口</nz-form-label>
                            <nz-form-control>
                              <nz-input-number formControlName="port" [nzMin]="1" [nzMax]="65535" nzPlaceHolder="22" style="width: 100%;"></nz-input-number>
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        </div>
                        <div class="form-row">
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">用户名</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="username" placeholder="SFTP 用户名" />
                            </nz-form-control>
                          </nz-form-item>
                        </div>
                        <div class="half">
                          <nz-form-item>
                            <nz-form-label [nzRequired]="true">远程路径</nz-form-label>
                            <nz-form-control>
                              <input nz-input formControlName="remotePath" placeholder="/backup/path" />
                            </nz-form-control>
                          </nz-form-item>
                      </div>
                      </div>
                    </div>
                        </form>
                      </nz-collapse-panel>

                      <nz-collapse-panel nzHeader="保留策略" [nzActive]="false">
                        <form nz-form [formGroup]="retentionForm" nzLayout="vertical">
                          <div style="margin-bottom: 16px;">
                            <label nz-checkbox formControlName="enableRetention">启用保留策略</label>
                            <div style="color: #666; font-size: 12px; margin-top: 4px;">
                              配置备份文件的自动清理规则，避免存储空间过度占用
                            </div>
                          </div>
                          
                          <div *ngIf="retentionForm.get('enableRetention')?.value">
                            <nz-alert 
                              nzType="info" 
                              nzMessage="保留策略说明" 
                              nzDescription="系统会按照以下规则保留备份文件，达到任一条件即触发清理。建议根据业务需求合理配置。"
                              nzShowIcon
                              style="margin-bottom: 16px;">
                            </nz-alert>
                            
                            <div nz-row [nzGutter]="16">
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>保留数量</nz-form-label>
                                  <nz-form-control>
                                    <nz-input-number 
                                      formControlName="retain" 
                                      [nzMin]="1" 
                                      [nzMax]="999" 
                                      nzPlaceHolder="10"
                                      style="width: 100%;">
                                    </nz-input-number>
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>保留天数</nz-form-label>
                                  <nz-form-control>
                                    <nz-input-number 
                                      formControlName="retainDays" 
                                      [nzMin]="1" 
                                      [nzMax]="365" 
                                      nzPlaceHolder="30"
                                      style="width: 100%;">
                                    </nz-input-number>
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>保留小时</nz-form-label>
                                  <nz-form-control>
                                    <nz-input-number 
                                      formControlName="retainHours" 
                                      [nzMin]="1" 
                                      [nzMax]="8760" 
                                      nzPlaceHolder="72"
                                      style="width: 100%;">
                                    </nz-input-number>
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                            </div>
                          </div>
                        </form>
                      </nz-collapse-panel>

                      <nz-collapse-panel nzHeader="资源配置" [nzActive]="false">
                        <form nz-form [formGroup]="resourceForm" nzLayout="vertical">
                          <div style="margin-bottom: 16px;">
                            <label nz-checkbox formControlName="enableResourceLimits">启用资源限制</label>
                            <div style="color: #666; font-size: 12px; margin-top: 4px;">
                              为备份任务分配合适的计算资源，确保备份性能和集群稳定性
                            </div>
                          </div>
                          
                          <div *ngIf="resourceForm.get('enableResourceLimits')?.value">
                            <nz-divider nzText="资源请求 (Requests)" nzOrientation="left"></nz-divider>
                            <div nz-row [nzGutter]="16">
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>CPU</nz-form-label>
                                  <nz-form-control>
                                    <input nz-input formControlName="requestsCpu" placeholder="例如：100m" />
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>Memory</nz-form-label>
                                  <nz-form-control>
                                    <input nz-input formControlName="requestsMemory" placeholder="例如：256Mi" />
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>Storage</nz-form-label>
                                  <nz-form-control>
                                    <input nz-input formControlName="requestsStorage" placeholder="例如：1Gi" />
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                            </div>

                            <nz-divider nzText="资源上限 (Limits)" nzOrientation="left"></nz-divider>
                            <div nz-row [nzGutter]="16">
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>CPU</nz-form-label>
                                  <nz-form-control>
                                    <input nz-input formControlName="limitsCpu" placeholder="例如：500m" />
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>Memory</nz-form-label>
                                  <nz-form-control>
                                    <input nz-input formControlName="limitsMemory" placeholder="例如：512Mi" />
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                              <div nz-col [nzSpan]="8">
                                <nz-form-item>
                                  <nz-form-label>Storage</nz-form-label>
                                  <nz-form-control>
                                    <input nz-input formControlName="limitsStorage" placeholder="例如：2Gi" />
                                  </nz-form-control>
                                </nz-form-item>
                              </div>
                            </div>
                          </div>
                        </form>
                      </nz-collapse-panel>
                    </nz-collapse>

                    <!-- 操作按钮 -->
                    <div class="form-actions">
                      <button nz-button
                              nzType="primary"
                              [nzLoading]="loadingService.isLoading(loadingKeys.XSTORE_BACKUP_CREATE)"
                              [disabled]="!isFormValid()"
                              (click)="saveBackup()">
                        <i nz-icon [nzType]="data.mode === 'edit' ? 'edit' : 'plus'"></i>
                        {{ data.mode === 'edit' ? '更新配置' : '创建配置' }}
                      </button>
                      <button nz-button
                              nzType="default"
                              (click)="switchToListTab()"
                              style="margin-left: 12px;">
                        <i nz-icon nzType="arrow-left"></i>
                        返回列表
                      </button>
                      <button nz-button
                              nzType="default"
                              (click)="resetForms()"
                              style="margin-left: 12px;">
                        <i nz-icon nzType="reload"></i>
                        重置
                      </button>
                    </div>
                  </nz-card>
                </div>
              </ng-template>
            </nz-tab>
          </nz-tabset>
        </div>
      </div>
  `,
  styles: [`
    .xstore-backup-management {
      padding: 16px;
      background: #f5f5f5;
      min-height: 100vh;
    }
    
    .page-header {
      margin-bottom: 16px;
    }
    
    .header-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .page-title {
      font-size: 24px;
      font-weight: 600;
      margin: 0 0 8px 0;
      color: #262626;
      display: flex;
      align-items: center;
      gap: 12px;
    }
    
    .page-icon {
      font-size: 28px;
      color: #1890ff;
    }
    
    .page-description {
      color: rgba(0, 0, 0, 0.6);
      font-size: 14px;
      margin: 0;
      line-height: 1.5;
    }
    
    .page-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .main-tabs {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .tab-content {
      padding: 16px;
    }
    
    .list-card {
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    
    .extra-actions {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    
    .view-toggle {
      margin-left: 16px;
      display: flex;
      gap: 8px;
    }
    
    .list-content {
      margin-top: 16px;
    }
    
    .backup-name {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .xstore-display {
      display: flex;
      align-items: center;
      gap: 8px;
      max-width: 100%;
    }
    
    .xstore-icon {
      font-size: 14px;
      color: #722ed1;
      flex-shrink: 0;
    }
    
    .xstore-name {
      color: #722ed1;
      font-weight: 500;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    
    .category-display {
      display: flex;
      align-items: center;
      justify-content: flex-start;
      max-width: 100%;
    }
    
    .category-tag {
      max-width: 100%;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    
    .action-buttons {
      display: flex;
      gap: 4px;
      justify-content: center;
    }
    
    .empty-state {
      text-align: center;
      padding: 40px 0;
    }
    
    .form-card {
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    }
    
    .form-actions {
      margin-top: 24px;
      text-align: center;
    }
    
    .config-summary {
      background: #f5f5f5;
      padding: 12px;
      border-radius: 6px;
      margin-bottom: 16px;
      font-family: monospace;
      font-size: 12px;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .xstore-backup-management {
        padding: 12px;
      }
      
      .tab-content {
        padding: 12px;
      }
      
      .extra-actions {
        flex-direction: column;
        align-items: stretch;
      }
      
      .action-buttons {
        flex-direction: column;
        gap: 8px;
      }
    }
  `]
})
export class XStoreBackupManagementComponent implements OnInit, OnDestroy {
  private fb = inject(FormBuilder);
  private apiService = inject(ApiService);
  public loadingService = inject(LoadingService);
  private ns = inject(NamespaceService);
  private message = inject(NzMessageService);
  loadingKeys = LoadingKeys;

  // Forms
  backupForm!: FormGroup;
  storageForm!: FormGroup;
  retentionForm!: FormGroup;
  resourceForm!: FormGroup;
  binlogForm!: FormGroup;

  // Data
  availableXStores: XStore[] = [];
  hpfsSinks: Array<{ name: string; type: string; endpoint?: string; bucket?: string; host?: string; port?: number; rootPath?: string; bucketLookupType?: string }> = [];
  sinkStatus: 'idle' | 'valid' | 'invalid' | 'checking' = 'idle';
  backups: XStoreBackupWithStatus[] = [];
  binlogBackups: XStoreBackupBinlog[] = [];
  // 汇总/明细视图切换，默认汇总
  viewMode: 'summary' | 'detail' = 'summary';
  // Binlog modal
  binlogModalVisible = false;
  // 明细计数与名称用于汇总视图显示类别细分（DN/GMS）
  private detailCountsByParent: Record<string, { dn: number; gms: number; dnNames: string[]; gmsNames: string[] }> = {};
  
  // UI State
  isProcessing = false;
  selectedTab = 0;
  showSecret = false;
  private destroy$ = new Subject<void>();
  
  // Data object for dialog compatibility
  data: { mode: 'create' | 'edit' | 'view'; backup?: XStoreBackup; namespace?: string } = { mode: 'create', namespace: 'default' };
  // Examples for HPFS ConfigMap snippets
  exampleOssYaml: string = `sinks:\n  - name: default\n    type: oss\n    endpoint: oss-cn-beijing.aliyuncs.com\n    accessKey: <OSS_AK>\n    accessSecret: <OSS_SK>\n    bucket: my-bucket\n`;
  exampleS3Yaml: string = `sinks:\n  - name: default\n    type: s3\n    endpoint: play.min.io\n    useSSL: true\n    bucketLookupType: dns\n    accessKey: <S3_AK>\n    secretKey: <S3_SK>\n    bucket: my-bucket\n`;
  exampleSftpYaml: string = `sinks:\n  - name: default\n    type: sftp\n    host: sftp.example.com\n    port: 22\n    user: backup\n    password: <SFTP_PASSWORD>\n    rootPath: /data/backup\n`;
  
  // Table configuration
  displayedColumns: string[] = [
    'name', 
    'xStoreName', 
    'backupType', 
    'phase', 
    'size', 
    'creationTime',
    'status',
    'actions'
  ];

  // Options
  backupTypes = [
    { value: 'full', label: '全量备份' },
    { value: 'incremental', label: '增量备份' }
  ];

  storageProviders = [
    { value: 'oss', label: '阿里云 OSS' },
    { value: 's3', label: 'Amazon S3' },
    { value: 'sftp', label: 'SFTP' }
  ];

  cpuOptions = ['0.5', '1', '2', '4', '8'];
  memoryOptions = ['1Gi', '2Gi', '4Gi', '8Gi', '16Gi'];
  storageOptions = ['10Gi', '20Gi', '50Gi', '100Gi', '200Gi'];

  constructor() {
    this.initializeForms();
  }

  ngOnInit(): void {
    this.ns.activeNamespace$.pipe(takeUntil(this.destroy$)).subscribe((namespace: string | null) => {
      this.data.namespace = namespace || 'default';
      this.backupForm.patchValue({ namespace: this.data.namespace });
      this.loadBackups();
      this.loadBinlogBackups();
    });
    this.loadInitialData();
    if (this.data.mode === 'edit' && this.data.backup) {
      this.populateFormFromBackup(this.data.backup);
    }
    // init binlog form
    this.binlogForm = this.fb.group({
      name: ['', Validators.required],
      namespace: [this.data.namespace || 'default'],
      xstoreName: ['', Validators.required],
      remoteExpireLogHours: [168, [Validators.min(1)]],
      localExpireLogHours: [7, [Validators.min(1)]],
      maxLocalBinlogCount: [60, [Validators.min(1)]],
      pointInTimeRecover: [true],
      binlogChecksum: ['CRC32'],
      storageType: ['oss', Validators.required],
      sinkName: ['', Validators.required]
    });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }

  private initializeForms(): void {
    // Main backup configuration form
    this.backupForm = this.fb.group({
      name: ['', [
        Validators.required, 
        Validators.pattern(/^[a-z0-9]([-a-z0-9]*[a-z0-9])?$/),
        Validators.maxLength(30)
      ]],
      namespace: [this.data.namespace || 'default', Validators.required],
      xStoreName: ['', Validators.required],
      backupType: ['full', Validators.required],
      schedule: [''],
      compression: [true],
      enableEncryption: [false]
    });

    // Storage configuration form
    this.storageForm = this.fb.group({
      storageType: ['oss', Validators.required],
      sinkName: ['', Validators.required],
      ossConfig: this.fb.group({
        accessKeyId: ['', Validators.required],
        accessKeySecret: ['', Validators.required],
        bucket: ['', Validators.required],
        endpoint: ['', Validators.required],
        prefix: ['']
      }),
      s3Config: this.fb.group({
        accessKeyId: ['', Validators.required],
        secretAccessKey: ['', Validators.required],
        bucket: ['', Validators.required],
        region: ['', Validators.required],
        endpoint: [''],
        prefix: ['']
      }),
      sftpConfig: this.fb.group({
        host: ['', Validators.required],
        port: [22, [Validators.required, Validators.min(1), Validators.max(65535)]],
        username: ['', Validators.required],
        password: [''],
        privateKey: [''],
        remotePath: ['', Validators.required]
      })
    });

    // Retention policy form
    this.retentionForm = this.fb.group({
      enableRetention: [false],
      retain: [7, [Validators.min(1)]],
      retainDays: [30, [Validators.min(1)]],
      retainHours: [168, [Validators.min(1)]]
    });

    // Resource configuration form
    this.resourceForm = this.fb.group({
      enableResourceLimits: [false],
      requestsCpu: ['1'],
      requestsMemory: ['2Gi'],
      requestsStorage: ['10Gi'],
      limitsCpu: ['2'],
      limitsMemory: ['4Gi'],
      limitsStorage: ['20Gi'],
      nodeSelector: this.fb.group({
        enabled: [false],
        key: [''],
        value: ['']
      })
    });

    // Watch for form changes
    this.storageForm.get('storageType')?.valueChanges.subscribe(() => {
      this.updateStorageValidators();
    });

    this.retentionForm.get('enableRetention')?.valueChanges.subscribe(enabled => {
      this.updateRetentionValidators(enabled);
    });

    this.resourceForm.get('enableResourceLimits')?.valueChanges.subscribe(enabled => {
      this.updateResourceValidators(enabled);
    });

    this.backupForm.get('enableEncryption')?.valueChanges.subscribe(enabled => {
      this.updateEncryptionValidators(enabled);
    });
  }

  private updateStorageValidators(): void {
    const storageType = this.storageForm.get('storageType')?.value;

    // Clear all validators first
    ['ossConfig','s3Config','sftpConfig'].forEach(groupName => {
      const configGroup = this.storageForm.get(groupName) as FormGroup;
      if (!configGroup) return;
      Object.keys(configGroup.controls).forEach(field => {
        const control = configGroup.get(field);
        control?.clearValidators();
        control?.updateValueAndValidity();
      });
    });
    // 后端契约：仅需 storageProvider.storageName + storageProvider.sink
    const sinkCtl = this.storageForm.get('sinkName');
    sinkCtl?.setValidators([Validators.required]);
    sinkCtl?.updateValueAndValidity();
  }

  private updateRetentionValidators(enabled: boolean): void {
    const fields = ['retain', 'retainDays', 'retainHours'];
    fields.forEach(field => {
      const control = this.retentionForm.get(field);
      if (enabled) {
        control?.setValidators([Validators.required, Validators.min(1)]);
      } else {
        control?.clearValidators();
      }
      control?.updateValueAndValidity();
    });
  }

  private updateResourceValidators(enabled: boolean): void {
    const resourceFields = [
      'requestsCpu', 'requestsMemory', 'requestsStorage',
      'limitsCpu', 'limitsMemory', 'limitsStorage'
    ];

    resourceFields.forEach(field => {
      const control = this.resourceForm.get(field);
      if (enabled) {
        control?.setValidators([Validators.required]);
      } else {
        control?.clearValidators();
      }
      control?.updateValueAndValidity();
    });
  }

  private updateEncryptionValidators(enabled: boolean): void {
    // Could add encryption-specific validators here
    // For now, encryption is just a boolean flag
  }

  private async loadInitialData(): Promise<void> {
    try {
      // Load available XStores
      this.availableXStores = await this.apiService.getXStores(this.data.namespace).toPromise() || [];
      
      // Load HPFS sinks for selection
      const sinksResp = await this.apiService.getHpfsSinks().toPromise();
      this.hpfsSinks = (sinksResp?.sinks || []).filter((s: any) => !!s?.name && !!s?.type);

      // Wire sink validation on change
      const sinkCtl = this.storageForm.get('sinkName');
      const typeCtl = this.storageForm.get('storageType');
      if (sinkCtl && typeCtl) {
        sinkCtl.valueChanges.pipe(debounceTime(200), distinctUntilChanged(), takeUntil(this.destroy$)).subscribe(async (val: string) => {
          this.sinkStatus = 'checking';
          try {
            const name = (val || '').trim();
            const type = (typeCtl.value || '').toString();
            if (!name || !type) { this.sinkStatus = 'idle'; return; }
            const res = await this.apiService.validateSink(name, type).toPromise();
            this.sinkStatus = (res?.status === 'ok') ? 'valid' : 'invalid';
          } catch {
            this.sinkStatus = 'invalid';
          }
        });
        typeCtl.valueChanges.pipe(takeUntil(this.destroy$)).subscribe(() => {
          // Reset sink selection when type changes
          sinkCtl.setValue('');
          this.sinkStatus = 'idle';
        });
      }

      // Load existing backups
      await this.loadBackups();
    } catch (error) {
      console.error('Failed to load initial data:', error);
    }
  }

  private async loadBackups(): Promise<void> {
    try {
      if (this.viewMode === 'summary') {
        const [summary, detail] = await Promise.all([
          this.apiService.listXStoreBackups(this.data.namespace as string, 'summary').toPromise(),
          this.apiService.listXStoreBackups(this.data.namespace as string, 'detail').toPromise()
        ]);
        const safeSummary = summary || [];
        const safeDetail = detail || [];
        // 统计明细类别计数
        this.detailCountsByParent = {};
        for (const b of safeDetail as any[]) {
          const name: string = b?.metadata?.name || '';
          const key = this.getParentKey(name);
          if (!this.detailCountsByParent[key]) this.detailCountsByParent[key] = { dn: 0, gms: 0, dnNames: [], gmsNames: [] };
          if (name.includes('-dn-')) { this.detailCountsByParent[key].dn++; this.detailCountsByParent[key].dnNames.push(name); }
          if (name.endsWith('-gms') || name.includes('-gms-')) { this.detailCountsByParent[key].gms++; this.detailCountsByParent[key].gmsNames.push(name); }
        }
        this.backups = safeSummary.map(backup => this.enrichBackupWithStatus(backup as any));
      } else {
        const backups = await this.apiService.listXStoreBackups(this.data.namespace as string, 'detail').toPromise() || [];
        this.backups = backups.map(backup => this.enrichBackupWithStatus(backup));
      }
    } catch (error) {
      console.error('Failed to load XStore backups:', error);
    }
  }

  private enrichBackupWithStatus(backup: XStoreBackup): XStoreBackupWithStatus {
    const status = backup.status;
    const provider: any = (backup as any).spec?.storageProvider || {};
    const storageName: string = (provider.type || provider.storageName || '').toString();

    return {
      ...backup,
      isRunning: status?.phase === 'Running',
      isCompleted: status?.phase === 'Completed',
      isFailed: status?.phase === 'Failed',
      displayStatus: this.getDisplayStatus(status?.phase as any, (status as any)?.stage),
      displaySize: this.formatBytes((status as any)?.backupSize),
      displayDuration: this.calculateDuration(status?.startTime as any, (status as any)?.completionTime),
      canRestore: status?.phase === 'Completed',
      storageType: storageName ? storageName.toUpperCase() : undefined,
      compressionRatio: this.calculateCompressionRatio((status as any)?.backupSize, (status as any)?.compressedSize)
    } as any;
  }

  private getDisplayStatus(phase?: string, stage?: string): string {
    if (!phase) return '未知';

    // 兼容 XStoreBackup 与旧/新阶段名称
    const map: Record<string, string> = {
      Pending: '等待中',
      Running: '备份中',
      Completed: '已完成',
      Failed: '失败',

      // XStore 专有阶段
      Backuping: '备份中',
      Collecting: '收集中',
      Calculating: '计算中',
      Binloging: '日志备份中',
      MetadataBackuping: '元数据备份中',
      Waiting: '等待中',
      Finished: '已完成',
      Deleting: '删除中',
      Dummy: '占位'
    };

    let status = map[phase] || phase;
    if (stage && stage !== phase) status += ` (${stage})`;
    return status;
  }

  private formatBytes(bytes?: number): string {
    if (!bytes) return '-';
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${sizes[i]}`;
  }

  private calculateDuration(startTime?: string, endTime?: string): string {
    if (!startTime) return '-';
    
    const start = new Date(startTime);
    const end = endTime ? new Date(endTime) : new Date();
    const diffMs = end.getTime() - start.getTime();
    
    const hours = Math.floor(diffMs / (1000 * 60 * 60));
    const minutes = Math.floor((diffMs % (1000 * 60 * 60)) / (1000 * 60));
    
    if (hours > 0) {
      return `${hours}h ${minutes}m`;
    } else {
      return `${minutes}m`;
    }
  }

  private calculateCompressionRatio(originalSize?: number, compressedSize?: number): number {
    if (!originalSize || !compressedSize) return 0;
    return Math.round((1 - compressedSize / originalSize) * 100);
  }

  getXStoreName(b: any): string {
    // Prefer spec.xStoreName; fallback to parsing from legacy names like "<backupName>-<xstoreName>-<role>"
    const direct = b?.spec?.xStoreName || (b?.spec?.xstore?.name);
    if (direct) return direct;
    const name: string = b?.metadata?.name || '';
    // Try to strip suffix like "-dn-0" or "-gms"
    const parts = name.split('-');
    if (parts.length > 4) {
      // Heuristic: remove trailing 3 segments for role/pod index
      return parts.slice(0, parts.length - 3).join('-');
    }
    return '未知';
  }

  private getParentKey(name: string): string {
    const dnIdx = name.lastIndexOf('-dn-');
    if (dnIdx > 0) return name.substring(0, dnIdx);
    if (name.endsWith('-gms')) return name.substring(0, name.length - 4);
    const gmsMid = name.lastIndexOf('-gms-');
    if (gmsMid > 0) return name.substring(0, gmsMid);
    return name;
  }

  getCategoryDisplay(b: XStoreBackupWithStatus): string {
    const name = b?.metadata?.name || '';
    if (this.viewMode === 'detail') {
      if (name.includes('-dn-')) return 'DN';
      if (name.endsWith('-gms') || name.includes('-gms-')) return 'GMS';
      return '未知';
    }
    // 汇总模式：显示混合及计数
    const key = this.getParentKey(name);
    const c = this.detailCountsByParent[key] || { dn: 0, gms: 0, dnNames: [], gmsNames: [] };
    if (c.dn > 0 && c.gms > 0) return `混合 (DN ${c.dn}, GMS ${c.gms})`;
    if (c.dn > 0) return `DN (${c.dn})`;
    if (c.gms > 0) return `GMS (${c.gms})`;
    return '未知';
  }

  getCategoryTooltip(b: XStoreBackupWithStatus): string {
    const name = b?.metadata?.name || '';
    if (this.viewMode === 'detail') {
      if (name.includes('-dn-')) return 'DN';
      if (name.endsWith('-gms') || name.includes('-gms-')) return 'GMS';
      return '';
    }
    const key = this.getParentKey(name);
    const c = this.detailCountsByParent[key];
    if (!c) return '';
    const parts: string[] = [];
    if (c.dnNames?.length) parts.push(`DN: ${c.dnNames.join(', ')}`);
    if (c.gmsNames?.length) parts.push(`GMS: ${c.gmsNames.join(', ')}`);
    return parts.join(' | ');
  }

  private populateFormFromBackup(backup: XStoreBackup): void {
    // Populate main form
    this.backupForm.patchValue({
      name: backup.metadata.name,
      namespace: backup.metadata.namespace,
      xStoreName: backup.spec.xStoreName,
      backupType: backup.spec.backupType,
      schedule: backup.spec.schedule,
      compression: backup.spec.compression,
      enableEncryption: backup.spec.encryption?.enabled
    });

    // Populate storage form
    const storageProvider = backup.spec.storageProvider;
    this.storageForm.patchValue({
      storageType: storageProvider.type
    });

    switch (storageProvider.type) {
      case 'oss':
        this.storageForm.get('ossConfig')?.patchValue(storageProvider.oss || {});
        break;
      case 's3':
        this.storageForm.get('s3Config')?.patchValue(storageProvider.s3 || {});
        break;
      case 'sftp':
        this.storageForm.get('sftpConfig')?.patchValue(storageProvider.sftp || {});
        break;
    }

    // Populate retention form
    const retentionPolicy = backup.spec.retentionPolicy;
    if (retentionPolicy) {
      this.retentionForm.patchValue({
        enableRetention: true,
        retain: retentionPolicy.retain,
        retainDays: retentionPolicy.retainDays,
        retainHours: retentionPolicy.retainHours
      });
    }

    // Populate resource form
    const resources = backup.spec.resources;
    if (resources) {
      this.resourceForm.patchValue({
        enableResourceLimits: true,
        requestsCpu: resources.requests?.cpu,
        requestsMemory: resources.requests?.memory,
        requestsStorage: resources.requests?.storage,
        limitsCpu: resources.limits?.cpu,
        limitsMemory: resources.limits?.memory,
        limitsStorage: resources.limits?.storage
      });
    }

    // Populate node selector
    const nodeSelector = backup.spec.nodeSelector;
    if (nodeSelector && Object.keys(nodeSelector).length > 0) {
      const firstKey = Object.keys(nodeSelector)[0];
      this.resourceForm.patchValue({
        nodeSelector: {
          enabled: true,
          key: firstKey,
          value: nodeSelector[firstKey]
        }
      });
    }
  }

  // Create or update XStore backup
  async saveBackup(): Promise<void> {
    if (!this.isFormValid()) {
      return;
    }

    this.isProcessing = true;

    try {
      const request = this.buildBackupRequest();

      if (this.data.mode === 'create') {
        await this.apiService.createXStoreBackup(request.namespace, request).toPromise();
      } else if (this.data.mode === 'edit' && this.data.backup) {
        const updatedBackup = this.buildUpdatedBackup(request);
        await this.apiService.updateXStoreBackup(request.namespace, updatedBackup).toPromise();
      }

      this.message.success(this.data.mode === 'edit' ? '更新成功' : '创建成功');
      
      // Reset form and switch to list tab
      this.resetForms();
      this.switchToListTab();
      
      // Reload the backups list
      await this.loadBackups();
    } catch (error) {
      console.error('Failed to save XStore backup:', error as any);
      const err: any = error;
      this.message.error(`操作失败: ${err?.error?.message || err?.message || '未知错误'}`);
    } finally {
      this.isProcessing = false;
    }
  }

  private buildBackupRequest(): any & { namespace: string } {
    const backupValue = this.backupForm.value;
    const storageValue = this.storageForm.value;
    const retentionValue = this.retentionForm.value;
    const resourceValue = this.resourceForm.value;

    // Use sink NAME configured in HPFS (not URL). Default to 'default'.
    const storageType: 'oss' | 's3' | 'sftp' = storageValue.storageType;
    const sink = (storageValue.sinkName || localStorage.getItem('xstoreBackupSinkName') || localStorage.getItem('backupSinkName') || 'default').trim();

    // Convert retention to duration string (hours) if enabled
    let retentionTime: string | undefined;
    if (retentionValue?.enableRetention) {
      const days = Number(retentionValue.retainDays) || 0;
      const hours = Number(retentionValue.retainHours) || 0;
      const totalHours = days * 24 + hours;
      if (totalHours > 0) retentionTime = `${totalHours}h`;
    }

    // Build CR object matching backend
    const cr: any = {
      apiVersion: 'polardbx.aliyun.com/v1',
      kind: 'XStoreBackup',
      metadata: {
        name: backupValue.name,
        namespace: backupValue.namespace
      },
      spec: {
        xstore: { name: backupValue.xStoreName },
        storageProvider: { storageName: storageType, sink: sink },
        preferredBackupRole: 'follower'
      }
    };
    if (retentionTime) cr.spec.retentionTime = retentionTime;

    return { namespace: backupValue.namespace, ...cr };
  }

  private buildUpdatedBackup(request: CreateXStoreBackupRequest): XStoreBackup {
    return {
      ...this.data.backup!,
      spec: {
        xStoreName: request.xStoreName,
        backupType: request.backupType,
        storageProvider: request.storageProvider,
        retentionPolicy: request.retentionPolicy,
        resources: request.resources,
        schedule: request.schedule,
        compression: request.compression,
        encryption: request.encryption,
        nodeSelector: request.nodeSelector
      }
    };
  }

  // Delete XStore backup
  async deleteBackup(backup: XStoreBackupWithStatus): Promise<void> {
    try {
      await this.apiService.deleteXStoreBackup(
        backup.metadata.namespace as string, 
        backup.metadata.name
      ).toPromise();
      
      this.message.success('删除成功');
      await this.loadBackups();
    } catch (error) {
      console.error('Failed to delete XStore backup:', error);
      this.message.error('删除失败');
    }
  }

  async forceDeleteBackup(backup: XStoreBackupWithStatus): Promise<void> {
    try {
      await this.apiService.forceDeleteXStoreBackup(
        backup.metadata.namespace as string,
        backup.metadata.name
      ).toPromise();
      
      this.message.success('强制删除成功');
      await this.loadBackups();
    } catch (error) {
      console.error('Failed to force delete XStore backup:', error);
      this.message.error('强制删除失败');
    }
  }

  // Other methods (edit, view, refresh, etc.)
  editBackup(backup: XStoreBackupWithStatus): void {
    this.data.mode = 'edit';
    this.data.backup = backup;
    this.populateFormFromBackup(backup);
    this.selectedTab = 2; // 跳转到"创建备份配置"Tab
  }

  viewBackup(backup: XStoreBackupWithStatus): void {
    console.log('View backup details:', backup);
  }

  async refreshBackups(): Promise<void> {
    await this.loadBackups();
  }

  getStatusColor(phase?: string): string {
    if (!phase) return '';
    const ok = ['Completed', 'Finished'];
    const running = ['Running', 'Pending', 'Backuping', 'Collecting', 'Calculating', 'Binloging', 'MetadataBackuping', 'Waiting', 'Deleting'];
    const fail = ['Failed'];
    if (ok.includes(phase)) return 'primary';
    if (fail.includes(phase)) return 'warn';
    if (running.includes(phase)) return 'accent';
    return '';
  }

  // Map material chip color semantics to ng-zorro tag colors
  getNzStatusColor(phase?: string): string {
    if (!phase) return 'default';
    const ok = ['Completed', 'Finished'];
    const running = ['Running', 'Pending', 'Backuping', 'Collecting', 'Calculating', 'Binloging', 'MetadataBackuping', 'Waiting', 'Deleting'];
    const fail = ['Failed'];
    if (ok.includes(phase)) return 'green';
    if (fail.includes(phase)) return 'red';
    if (running.includes(phase)) return 'blue';
    return 'default';
  }

  getProgressPercentage(backup: XStoreBackupWithStatus): number {
    return backup.status?.progress?.percentage || 0;
  }

  onChangeViewMode(mode: 'summary' | 'detail'): void {
    if (this.viewMode !== mode) {
      this.viewMode = mode;
      this.loadBackups();
    }
  }

  isFormValid(): boolean {
    return this.backupForm.valid && 
           this.storageForm.valid &&
           (!this.retentionForm.get('enableRetention')?.value || this.retentionForm.valid) &&
           (!this.resourceForm.get('enableResourceLimits')?.value || this.resourceForm.valid);
  }

  switchToListTab(): void {
    this.selectedTab = 0;
  }

  switchToBinlogTab(): void {
    this.selectedTab = 1; // 跳转到"日志备份配置"Tab
  }
  
  formatDate(dateString?: string): string {
    if (!dateString) return '-';
    return new Date(dateString).toLocaleString('zh-CN');
  }

  getFieldError(formGroup: FormGroup, fieldName: string): string {
    const field = formGroup.get(fieldName);
    if (field?.errors && field.touched) {
      if (field.errors['required']) return '此字段为必填项';
      if (field.errors['pattern']) return '格式不正确';
      if (field.errors['min']) return `最小值为 ${field.errors['min'].min}`;
      if (field.errors['max']) return `最大值为 ${field.errors['max'].max}`;
    }
    return '';
  }

  createNew(): void {
    this.data.mode = 'create';
    this.resetForms();
    this.selectedTab = 2; // 跳转到"创建备份配置"Tab
  }

  // ============================================================================
  // XStoreBackupBinlog Methods (Standard Edition Incremental Log Backup)
  // ============================================================================

  private async loadBinlogBackups(): Promise<void> {
    try {
      const binlogs = await this.apiService.listXStoreBackupBinlogs(this.data.namespace as string).toPromise();
      this.binlogBackups = binlogs || [];
    } catch (error) {
      console.error('Failed to load binlog backups:', error);
      this.binlogBackups = [];
    }
  }

  async refreshBinlogBackups(): Promise<void> {
    await this.loadBinlogBackups();
  }

  createNewBinlog(): void {
    this.binlogForm.reset({
      namespace: this.data.namespace || 'default',
      remoteExpireLogHours: 168,
      localExpireLogHours: 7,
      maxLocalBinlogCount: 60,
      pointInTimeRecover: true,
      binlogChecksum: 'CRC32',
      storageType: 'oss'
    });
    this.binlogModalVisible = true;
  }

  viewBinlogBackup(binlog: XStoreBackupBinlog): void {
    // TODO: Implement binlog backup detail view
    this.message.info('增量日志备份详情查看功能开发中...');
  }

  editBinlogBackup(binlog: XStoreBackupBinlog): void {
    this.binlogForm.reset({
      name: binlog.metadata.name,
      namespace: binlog.metadata.namespace,
      xstoreName: binlog.spec.xstoreName,
      remoteExpireLogHours: binlog.spec.remoteExpireLogHours ?? 168,
      localExpireLogHours: binlog.spec.localExpireLogHours ?? 7,
      maxLocalBinlogCount: binlog.spec.maxLocalBinlogCount ?? 60,
      pointInTimeRecover: binlog.spec.pointInTimeRecover ?? true,
      binlogChecksum: binlog.spec.binlogChecksum ?? 'CRC32',
      storageType: binlog.spec.storageProvider?.storageName ?? 'oss',
      sinkName: binlog.spec.storageProvider?.sink ?? ''
    });
    this.binlogModalVisible = true;
  }

  async deleteBinlogBackup(binlog: XStoreBackupBinlog): Promise<void> {
    try {
      await this.apiService.deleteXStoreBackupBinlog(
        binlog.metadata.namespace as string,
        binlog.metadata.name
      ).toPromise();
      
      this.message.success('删除成功');
      await this.loadBinlogBackups();
    } catch (error) {
      console.error('Failed to delete binlog backup:', error);
      this.message.error('删除失败');
    }
  }

  async saveBinlogBackup(): Promise<void> {
    if (!this.binlogForm.valid) {
      this.message.warning('请完善表单信息');
      return;
    }
    const v = this.binlogForm.value;
    const namespace = (v.namespace || this.data.namespace || 'default') as string;
    const request: CreateXStoreBackupBinlogRequest = {
      metadata: {
        name: v.name,
        namespace
      },
      spec: {
        xstoreName: v.xstoreName,
        remoteExpireLogHours: v.remoteExpireLogHours,
        localExpireLogHours: v.localExpireLogHours,
        maxLocalBinlogCount: v.maxLocalBinlogCount,
        pointInTimeRecover: v.pointInTimeRecover,
        binlogChecksum: v.binlogChecksum,
        storageProvider: {
          storageName: v.storageType,
          sink: v.sinkName
        }
      }
    };
    try {
      await this.apiService.createXStoreBackupBinlog(namespace, request).toPromise();
      this.message.success('保存成功');
      this.binlogModalVisible = false;
      await this.loadBinlogBackups();
    } catch (error: any) {
      console.error('Failed to save binlog backup:', error);
      this.message.error(error?.error?.message || '保存失败');
    }
  }

  resetForms(): void {
    this.backupForm.reset({
      namespace: this.data.namespace || 'default',
      backupType: 'full',
      compression: true,
      enableEncryption: false
    });
    
    this.storageForm.reset({
      storageType: 'oss'
    });
    
    this.retentionForm.reset({
      enableRetention: false,
      retain: 7,
      retainDays: 30,
      retainHours: 168
    });
    
    this.resourceForm.reset({
      enableResourceLimits: false,
      requestsCpu: '1',
      requestsMemory: '2Gi',
      requestsStorage: '10Gi',
      limitsCpu: '2',
      limitsMemory: '4Gi',
      limitsStorage: '20Gi',
      nodeSelector: { enabled: false, key: '', value: '' }
    });
  }

  getTitle(): string {
    switch (this.data.mode) {
      case 'create': return '创建 XStore 备份';
      case 'edit': return '编辑 XStore 备份';
      case 'view': return 'XStore 备份详情';
      default: return 'XStore 备份管理';
    }
  }

  get selectedStorageType(): string {
    return this.storageForm.get('storageType')?.value || 'oss';
  }

  get filteredHpfsSinks(): Array<{ name: string; type: string; endpoint?: string; bucket?: string; host?: string; port?: number; rootPath?: string; bucketLookupType?: string }> {
    const type = (this.selectedStorageType || '').toLowerCase();
    if (!type) return this.hpfsSinks;
    return this.hpfsSinks.filter(s => (s.type || '').toLowerCase() === type);
  }

  get filteredHpfsSinksForBinlog(): Array<{ name: string; type: string; endpoint?: string; bucket?: string; host?: string; port?: number; rootPath?: string; bucketLookupType?: string }> {
    const type = (this.binlogForm?.get('storageType')?.value || '').toLowerCase();
    if (!type) return this.hpfsSinks;
    return this.hpfsSinks.filter(s => (s.type || '').toLowerCase() === type);
  }

  getStorageIcon(type: string): string {
    switch (type) {
      case 'oss': return 'cloud';
      case 's3': return 'cloud';
      case 'sftp': return 'share-alt';
      default: return 'database';
    }
  }

  getBackupTypeIcon(type: string): string {
    switch (type) {
      case 'full': return 'database';
      case 'incremental': return 'sync';
      default: return 'file-text';
    }
  }

  getConfigSummary(): string {
    const name = this.backupForm.get('name')?.value || '未设置';
    const xstore = this.backupForm.get('xStoreName')?.value || '未选择';
    const type = this.backupForm.get('backupType')?.value || '未选择';
    const storage = this.storageForm.get('storageType')?.value || '未选择';
    const schedule = this.backupForm.get('schedule')?.value ? '定时备份' : '立即备份';
    
    return `备份名称：${name} | 目标：${xstore} | 类型：${type} | 存储：${storage} | 模式：${schedule}`;
  }

  previewConfig(): void {
    // 可以在这里显示配置预览对话框
    console.log('预览配置:', {
      backup: this.backupForm.value,
      storage: this.storageForm.value,
      retention: this.retentionForm.value,
      resource: this.resourceForm.value
    });
  }
}