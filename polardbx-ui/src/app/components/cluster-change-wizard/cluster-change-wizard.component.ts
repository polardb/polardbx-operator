import { Component, OnInit, inject, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzResultModule } from 'ng-zorro-antd/result';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzMessageService } from 'ng-zorro-antd/message';
import { PrechangeCheckComponent } from '../prechange-check/prechange-check.component';
import { ApiService } from '../../services/api.service';

// Check → route mapping with applicable operation types
type OperationType = 'upgrade' | 'create' | 'normal';
const CheckRouteMap: Record<string, { route?: string; allow?: OperationType[]; note?: string }> = {
  checkClusterReady:       { route: '/clusters/:namespace/:name',               allow: ['upgrade','create','normal'] },
  checkControllersReady:   { route: '/operations/monitoring/health',            allow: ['upgrade','create','normal'] },
  checkPodsReady:          { route: '/operations/nodes',                        allow: ['upgrade','create','normal'] },
  checkStorage:            { route: '/backup/xstore-backups',                   allow: ['upgrade','create','normal'] },
  checkRecentBackup:       { route: '/backup/manual-backups',                   allow: ['upgrade','create','normal'] },
  checkRPO:                { route: '/backup/backup-binlogs',                   allow: ['upgrade','normal'] },
  checkConflicts:          { route: '/operations/system-tasks',                 allow: ['upgrade','create','normal'] },
  checkScheduling:         { route: '/operations/nodes',                        allow: ['upgrade','create','normal'] },
  checkNodeDiskPressure:   { route: '/operations/monitoring/health',            allow: ['upgrade','create','normal'] },
  checkResourceQuota:      { note: '暂无专页，请在命名空间配额中查看/调整',      allow: ['upgrade','create','normal'] },
  checkBinlogAvailable:    { route: '/backup/backup-binlogs',                   allow: ['upgrade','normal'] },
  checkRBAC:               { route: '/operations/monitoring/preflight',         allow: ['upgrade','create','normal'] },
  checkVersionCompat:      { route: '/clusters/:namespace/:name/change',        allow: ['upgrade'] }
};

@Component({
  selector: 'app-cluster-change-wizard',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    ReactiveFormsModule,
    NzCardModule,
    NzStepsModule,
    NzButtonModule,
    NzIconModule,
    NzFormModule,
    NzSelectModule,
    NzInputNumberModule,
    NzAlertModule,
    NzResultModule,
    NzTagModule,
    NzGridModule,
    NzSpinModule,
    PrechangeCheckComponent
  ],
  template: `
    <div class="wizard">
      <nz-card [nzTitle]="'集群变更向导'">
        <nz-steps [nzCurrent]="currentStep" nzDirection="horizontal">
          <nz-step nzTitle="预检" nzDescription="安全检查"></nz-step>
          <nz-step nzTitle="选择操作" nzDescription="升级/扩缩容/配置"></nz-step>
          <nz-step nzTitle="参数" nzDescription="填写变更参数"></nz-step>
          <nz-step nzTitle="影响评估" nzDescription="阻断项校验"></nz-step>
          <nz-step nzTitle="执行" nzDescription="执行与监控"></nz-step>
          <nz-step nzTitle="回验" nzDescription="健康与建议"></nz-step>
        </nz-steps>

        <div class="step-body" *ngIf="currentStep === 0">
          <app-prechange-check
            [embedded]="true"
            [initialNamespace]="namespace"
            [initialCluster]="name"
            [autoRun]="true"
            (completed)="onPrecheckCompleted($event)">
          </app-prechange-check>

          <div class="step-actions">
            <button nz-button nzType="default" (click)="goBackToCluster()">
              <i nz-icon nzType="left"></i>
              返回集群
            </button>
            <button nz-button nzType="primary" [disabled]="!precheckPass && !ackWarnings" (click)="next()">
              下一步
              <i nz-icon nzType="right"></i>
            </button>
            <label class="ack" *ngIf="!precheckPass && hasWarn">
              <input type="checkbox" [(ngModel)]="ackWarnings" /> 我已知晓警告并继续
            </label>
          </div>
        </div>

        <div class="step-body" *ngIf="currentStep === 1">
          <div nz-row nzGutter="16">
            <div nz-col [nzSpan]="8">
              <nz-card class="op-card" [nzBordered]="true" (click)="selectOp('scale')" [class.active]="opType==='scale'">
                <h3><i nz-icon nzType="desktop"></i> 扩缩容</h3>
                <p>调整 CN/DN 副本数，滚动方式，平滑扩缩</p>
                <nz-tag nzColor="blue">低风险</nz-tag>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="8">
              <nz-card class="op-card" [nzBordered]="true" (click)="selectOp('upgrade')" [class.active]="opType==='upgrade'">
                <h3><i nz-icon nzType="rocket"></i> 版本升级</h3>
                <p>选择目标版本并滚动升级</p>
                <nz-tag nzColor="orange">中风险</nz-tag>
              </nz-card>
            </div>
            <div nz-col [nzSpan]="8">
              <nz-card class="op-card" [nzBordered]="true" (click)="selectOp('config')" [class.active]="opType==='config'">
                <h3><i nz-icon nzType="setting"></i> 配置变更</h3>
                <p>修改关键参数，按需重启</p>
                <nz-tag nzColor="processing">可回滚</nz-tag>
              </nz-card>
            </div>
          </div>
          <div class="step-actions">
            <button nz-button nzType="default" (click)="prev()">
              <i nz-icon nzType="left"></i>
              上一步
            </button>
            <button nz-button nzType="primary" [disabled]="!opType" (click)="next()">
              下一步
              <i nz-icon nzType="right"></i>
            </button>
          </div>
        </div>

        <div class="step-body" *ngIf="currentStep === 2">
          <!-- 参数表单：按操作类型切换 -->
          <form nz-form [formGroup]="scaleForm" nzLayout="vertical" *ngIf="opType==='scale'">
            <div nz-row [nzGutter]="16">
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label nzRequired>CN 副本</nz-form-label>
                  <nz-form-control>
                    <nz-input-number formControlName="cnReplicas" [nzMin]="1" [nzMax]="32" style="width: 100%"></nz-input-number>
                  </nz-form-control>
                </nz-form-item>
              </div>
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label nzRequired>DN 副本</nz-form-label>
                  <nz-form-control>
                    <nz-input-number formControlName="dnReplicas" [nzMin]="1" [nzMax]="64" style="width: 100%"></nz-input-number>
                  </nz-form-control>
                </nz-form-item>
              </div>
            </div>
          </form>

          <form nz-form [formGroup]="upgradeForm" nzLayout="vertical" *ngIf="opType==='upgrade'">
            <nz-form-item>
              <nz-form-label>当前版本</nz-form-label>
              <nz-form-control>
                <span>{{ currentVersion || '-' }}</span>
              </nz-form-control>
            </nz-form-item>
            <nz-form-item>
              <nz-form-label nzRequired>目标版本</nz-form-label>
              <nz-form-control>
                <nz-select formControlName="targetVersion" nzPlaceHolder="请选择目标版本">
                  <nz-option *ngFor="let v of upgradeOptions; trackBy: trackByVersion" [nzValue]="v.version" [nzLabel]="vLabel(v)"></nz-option>
                </nz-select>
              </nz-form-control>
            </nz-form-item>
          </form>

          <form nz-form [formGroup]="configForm" nzLayout="vertical" *ngIf="opType==='config'">
            <div nz-row nzGutter="16">
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label>CN 日志级别</nz-form-label>
                  <nz-form-control>
                    <nz-select formControlName="cnLogLevel">
                      <nz-option nzValue="INFO" nzLabel="INFO"></nz-option>
                      <nz-option nzValue="WARN" nzLabel="WARN"></nz-option>
                      <nz-option nzValue="ERROR" nzLabel="ERROR"></nz-option>
                      <nz-option nzValue="DEBUG" nzLabel="DEBUG"></nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </div>
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label>DN 日志级别</nz-form-label>
                  <nz-form-control>
                    <nz-select formControlName="dnLogLevel">
                      <nz-option nzValue="INFO" nzLabel="INFO"></nz-option>
                      <nz-option nzValue="WARN" nzLabel="WARN"></nz-option>
                      <nz-option nzValue="ERROR" nzLabel="ERROR"></nz-option>
                      <nz-option nzValue="DEBUG" nzLabel="DEBUG"></nz-option>
                    </nz-select>
                  </nz-form-control>
                </nz-form-item>
              </div>
              <div nz-col [nzSpan]="8">
                <nz-form-item>
                  <nz-form-label>慢 SQL 阈值 (ms)</nz-form-label>
                  <nz-form-control>
                    <nz-input-number formControlName="slowLogThresholdMs" [nzMin]="100" [nzMax]="600000" [nzStep]="100" style="width: 100%"></nz-input-number>
                  </nz-form-control>
                </nz-form-item>
              </div>
            </div>
            <div nz-row>
              <div nz-col [nzSpan]="8">
                <label style="display:flex;align-items:center;gap:8px;">
                  <input type="checkbox" [(ngModel)]="_enableSqlAuditModel" (ngModelChange)="configForm.patchValue({ enableSqlAudit: _enableSqlAuditModel })" />
                  启用 SQL 审计
                </label>
              </div>
            </div>
          </form>

          <div class="step-actions">
            <button nz-button nzType="default" (click)="prev()">
              <i nz-icon nzType="left"></i>
              上一步
            </button>
            <button nz-button nzType="primary" [disabled]="!canProceedParams()" (click)="goToImpact()">
              下一步
              <i nz-icon nzType="right"></i>
            </button>
          </div>
        </div>

        <div class="step-body" *ngIf="currentStep === 3">
          <nz-spin [nzSpinning]="impactLoading">
            <div class="loading-tip" *ngIf="impactLoading">正在评估影响...</div>
            <nz-alert [nzType]="impactPass ? 'success' : (impactHasError ? 'error' : 'warning')" nzShowIcon
              [nzMessage]="impactPass ? '评估通过' : (impactHasError ? '存在阻断项' : '存在警告项')"
              [nzDescription]="impactDescription">
            </nz-alert>

            <div *ngIf="opType==='upgrade' && upgradeImpactSummary" style="margin-top:12px;">
              <nz-tag nzColor="processing">升级摘要</nz-tag>
              <span>{{ upgradeImpactSummary }}</span>
            </div>

            <div class="impact-list" *ngIf="impactPlan.length">
              <div class="impact-item" *ngFor="let it of impactPlan; trackBy: trackByIdx">
                <nz-tag [nzColor]="it.state==='ok' ? 'green' : (it.state==='warn' ? 'orange' : 'red')">{{ it.state }}</nz-tag>
                <span class="impact-id">{{ it.id }}</span>
                <span class="impact-msg">{{ it.message }}</span>
                <span class="actions" *ngIf="it.state!=='ok'">
                  <button nz-button nzType="link" (click)="navigateSuggested(it.id)">前往处理</button>
                </span>
              </div>
            </div>
          </nz-spin>
          <div class="step-actions">
            <button nz-button nzType="default" (click)="prev()">
              <i nz-icon nzType="left"></i>
              上一步
            </button>
            <button nz-button nzType="primary" [disabled]="impactHasError" (click)="next()">
              下一步
              <i nz-icon nzType="right"></i>
            </button>
          </div>
        </div>

        <div class="step-body" *ngIf="currentStep === 4">
          <nz-spin [nzSpinning]="executing">
            <div class="loading-tip" *ngIf="executing">正在执行变更...</div>
            <div class="execute" *ngIf="!executed">
              <button nz-button nzType="primary" (click)="startExecute()" [disabled]="!opType">
                <i nz-icon nzType="play-circle"></i>
                开始执行
              </button>
            </div>
            <div class="logs" *ngIf="logs.length">
              <pre>{{ logs.join('\n') }}</pre>
            </div>
          </nz-spin>
          <div class="step-actions">
            <button nz-button nzType="default" (click)="prev()" [disabled]="executing">
              <i nz-icon nzType="left"></i>
              上一步
            </button>
            <button nz-button nzType="primary" (click)="next()" [disabled]="!executed">
              下一步
              <i nz-icon nzType="right"></i>
            </button>
          </div>
        </div>

        <div class="step-body" *ngIf="currentStep === 5">
          <nz-result [nzStatus]="postPass ? 'success' : 'warning'" [nzTitle]="postPass ? '回验通过' : '回验存在警告'" [nzSubTitle]="postDescription"></nz-result>
          <div style="margin:12px 0;" *ngIf="podsTotal>=0">
            <nz-tag nzColor="blue">Pods</nz-tag>
            <span>就绪 {{ podsReady }}/{{ podsTotal }}</span>
          </div>
          <div class="step-actions">
            <button nz-button nzType="default" (click)="goBackToCluster()">
              返回集群
            </button>
            <button nz-button nzType="primary" (click)="navigateAfter()">
              查看建议页
            </button>
          </div>
        </div>
      </nz-card>
    </div>
  `,
  styles: [`
    .wizard { padding: 16px; }
    .step-body { margin-top: 16px; }
    .step-actions { margin-top: 16px; display: flex; gap: 8px; align-items: center; }
    .op-card { cursor: pointer; transition: all .2s; }
    .op-card.active { border-color: #1890ff; box-shadow: 0 0 0 2px rgba(24,144,255,.1); }
    .op-card h3 { margin-bottom: 8px; }
    .op-card p { margin: 0 0 8px 0; color: rgba(0,0,0,.65); display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
    .op-card { height: 140px; display: flex; flex-direction: column; justify-content: space-between; }
    .impact-list { margin-top: 12px; display: flex; flex-direction: column; gap: 8px; }
    .impact-item { padding: 8px; border: 1px solid #f0f0f0; border-radius: 6px; display: flex; gap: 8px; align-items: center; }
    .impact-id { font-weight: 500; }
    .impact-msg { color: rgba(0,0,0,.65); }
    .logs pre { background: #0b1021; color: #e6e6e6; padding: 12px; border-radius: 6px; min-height: 140px; }
    .ack { margin-left: auto; color: #faad14; }
    .loading-tip { text-align: center; color: rgba(0,0,0,0.65); letter-spacing: 0.5px; padding: 20px 0; font-size: 14px; }
  `]
})
export class ClusterChangeWizardComponent implements OnInit {
  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private fb = inject(FormBuilder);
  private api = inject(ApiService);
  private msg = inject(NzMessageService);

  @Input() namespace: string = 'default';
  @Input('clusterName') name: string = '';

  currentStep = 0;
  precheckPass = false;
  hasWarn = false;
  ackWarnings = false;

  opType: 'scale'|'upgrade'|'config' | null = null;
  scaleForm: FormGroup;
  upgradeForm: FormGroup;
  configForm: FormGroup;
  _enableSqlAuditModel = false;
  // 升级候选与当前版本
  currentVersion = '';
  upgradeOptions: Array<{ version: string; recommended?: boolean }> = [];
  upgradeImpactSummary = '';
  // 回验摘要
  podsReady: number = -1;
  podsTotal: number = -1;

  // impact/dry-run
  impactLoading = false;
  impactPass = false;
  impactHasError = false;
  impactDescription = '';
  impactPlan: Array<{ id: string; state: 'ok'|'warn'|'error'; message: string }> = [];
  impactToken = '';
  impactSig = '';

  // execute
  executing = false;
  executed = false;
  logs: string[] = [];

  // post verify
  postPass = false;
  postDescription = '';

  constructor() {
    this.scaleForm = this.fb.group({
      cnReplicas: [2, [Validators.required, Validators.min(1)]],
      dnReplicas: [2, [Validators.required, Validators.min(1)]]
    });
    this.upgradeForm = this.fb.group({
      targetVersion: ['5.4.19', [Validators.required]]
    });
    this.configForm = this.fb.group({
      cnLogLevel: ['INFO', [Validators.required]],
      dnLogLevel: ['INFO', [Validators.required]],
      slowLogThresholdMs: [2000, [Validators.required, Validators.min(100)]],
      enableSqlAudit: [false]
    });
  }

  ngOnInit(): void {
    this.route.params.subscribe(p => {
      this.namespace = p['namespace'] || this.namespace;
      this.name = p['name'] || this.name;
    });
    // 获取当前集群信息
    this.api.getCluster(this.namespace, this.name).subscribe({
      next: (c: any) => {
        this.currentVersion = (c?.status?.version || c?.status?.polardbxVersion || c?.spec?.version || '').toString();
      },
      error: () => {}
    });
    // 预取升级候选，填充下拉并默认选推荐项
    this.api.getClusterUpgradePlan(this.namespace, this.name).subscribe({
      next: (plan: any) => {
        const cands: Array<{ version: string; recommended?: boolean }> = Array.isArray(plan?.candidates) ? plan.candidates : [];
        this.currentVersion = plan?.currentVersion || this.currentVersion;
        this.upgradeOptions = cands;
        if (cands.length > 0) {
          const recommended = cands.find(c => c.recommended) || cands[0];
          const v = recommended?.version || cands[0]?.version;
          if (v) this.upgradeForm.patchValue({ targetVersion: v });
        }
      },
      error: () => {}
    });
  }

  onPrecheckCompleted(evt: { pass: boolean; hasWarn: boolean; hasError: boolean }): void {
    this.precheckPass = !!evt?.pass;
    this.hasWarn = !!evt?.hasWarn;
    this.ackWarnings = false;
  }

  next(): void { this.currentStep = Math.min(5, this.currentStep + 1); }
  prev(): void { this.currentStep = Math.max(0, this.currentStep - 1); }

  selectOp(op: 'scale'|'upgrade'|'config'): void { this.opType = op; }

  canProceedParams(): boolean {
    if (this.opType === 'scale') return this.scaleForm.valid;
    if (this.opType === 'upgrade') return this.upgradeForm.valid;
    if (this.opType === 'config') return this.configForm.valid;
    return false;
  }

  goToImpact(): void {
    this.currentStep = 3;
    this.evaluateImpact();
  }

  evaluateImpact(): void {
    this.impactLoading = true;
    this.impactPlan = [];
    const op: any = this.opType || 'config';
    const spec = this.opType === 'scale' ? this.scaleForm.value
      : this.opType === 'upgrade' ? this.upgradeForm.value
      : this.configForm.value;
    this.api.runPrecheck(this.namespace, this.name, op, spec).subscribe({
      next: (res: any) => {
        const plan = Array.isArray(res?.plan) ? res.plan : [];
        this.impactPlan = plan;
        this.impactToken = res?.token || '';
        this.impactSig = res?.tokenSig || '';
        const hasError = plan.some((p: any) => p.state === 'error');
        const hasWarn = plan.some((p: any) => p.state === 'warn');
        this.impactHasError = hasError;
        this.impactPass = !hasError;
        this.impactDescription = hasError ? '存在阻断项，请先处理后再继续'
          : hasWarn ? '存在警告项，可在知晓风险后继续'
          : '未发现阻断项，可继续执行';
        // 升级摘要
        if (this.opType === 'upgrade') {
          const tgt = this.upgradeForm.get('targetVersion')?.value;
          this.upgradeImpactSummary = `将以滚动策略升级：${this.currentVersion || '未知'} → ${tgt || '未选择'}，不可用阈值 1 个实例。`;
        } else {
          this.upgradeImpactSummary = '';
        }
        this.impactLoading = false;
      },
      error: (err) => {
        const status = err?.status;
        const message = status === 404 ? '后端未实现预检接口（404）' : '预检失败（网络/后端异常）';
        this.msg.warning(`${message}，已回退为最小评估`);
        this.impactPlan = [
          { id: 'checkStorage', state: 'warn', message: '请确认 HPFS Sink 已配置（备份模块）' },
          { id: 'checkRecentBackup', state: 'warn', message: '建议先完成一次全量备份' }
        ];
        this.impactHasError = false;
        this.impactPass = true;
        this.impactDescription = '最小评估通过，可继续（存在建议项）';
        this.impactLoading = false;
      }
    });
  }

  startExecute(): void {
    if (!this.opType) return;
    this.executing = true;
    this.logs = [];
    const log = (s: string) => { this.logs.push(`[${new Date().toLocaleTimeString()}] ${s}`); };
    if (this.opType === 'scale') {
      log('开始扩缩容任务');
      this.api.scaleCluster(this.namespace, this.name, this.scaleForm.value, this.impactToken, this.impactSig).subscribe({
        next: () => {
          log('扩缩容请求已提交');
          this.executing = false;
          this.executed = true;
          this.msg.success('扩缩容任务已启动');
          this.postVerify();
        },
        error: (e) => {
          log(`扩缩容失败: ${e?.message || '未知错误'}`);
          this.executing = false;
        }
      });
    } else if (this.opType === 'upgrade') {
      log('开始升级任务');
      this.api.upgradeCluster(this.namespace, this.name, this.upgradeForm.value, this.impactToken, this.impactSig).subscribe({
        next: () => {
          log('升级请求已提交');
          this.executing = false;
          this.executed = true;
          this.msg.success('升级任务已启动');
          this.postVerify();
        },
        error: (e) => {
          log(`升级失败: ${e?.message || '未知错误'}`);
          this.executing = false;
        }
      });
    } else if (this.opType === 'config') {
      log('开始应用配置变更');
      const cfg = this.configForm.value;
      const applyCn$ = this.api.updateClusterLogConfig(this.namespace, this.name, 'cn', {
        logLevel: cfg.cnLogLevel,
        enableSqlAudit: !!cfg.enableSqlAudit,
        slowLogThresholdMs: Number(cfg.slowLogThresholdMs)
      });
      const applyDn$ = this.api.updateClusterLogConfig(this.namespace, this.name, 'dn', {
        logLevel: cfg.dnLogLevel
      });
      applyCn$.subscribe({
        next: () => {
          log('CN 日志配置已更新');
          applyDn$.subscribe({
            next: () => {
              log('DN 日志配置已更新');
              this.executing = false;
              this.executed = true;
              this.msg.success('配置变更已提交');
              this.postVerify();
            },
            error: (e2) => {
              log(`DN 配置更新失败: ${e2?.message || '未知错误'}`);
              this.executing = false;
            }
          });
        },
        error: (e1) => {
          log(`CN 配置更新失败: ${e1?.message || '未知错误'}`);
          this.executing = false;
        }
      });
    }
  }

  postVerify(): void {
    // 简化回验：再次调用 config 类型预检
    this.api.runPrecheck(this.namespace, this.name, 'config').subscribe({
      next: (res: any) => {
        const plan = Array.isArray(res?.plan) ? res.plan : [];
        const hasError = plan.some((p: any) => p.state === 'error');
        const hasWarn = plan.some((p: any) => p.state === 'warn');
        this.postPass = !hasError;
        this.postDescription = hasError ? '仍有阻断项，请处理后再试'
          : hasWarn ? '存在警告项，建议尽快处理'
          : '集群状态良好';
      },
      error: () => {
        this.postPass = false;
        this.postDescription = '回验失败，请稍后重试';
      }
    });
    // 计算 Pods Ready 概览
    this.api.getPodsForCluster(this.namespace, this.name).subscribe({
      next: (pods: any[]) => {
        const total = Array.isArray(pods) ? pods.length : 0;
        const ready = (pods || []).filter((p: any) => (p?.status?.phase || '').toLowerCase() === 'running').length;
        this.podsTotal = total;
        this.podsReady = ready;
      },
      error: () => {
        this.podsTotal = -1;
        this.podsReady = -1;
      }
    });
  }

  navigateSuggested(id: string): void {
    const lower = (id || '').toLowerCase();
    const key = Object.keys(CheckRouteMap).find(k => lower.includes(k.replace('check','').toLowerCase()));
    const op: OperationType = this.opType === 'upgrade' ? 'upgrade' : 'normal';
    if (!key) { this.msg.info('请前往相关页面处理'); return; }
    const cfg = CheckRouteMap[key];
    if (cfg.allow && !cfg.allow.includes(op)) {
      this.msg.info(cfg.note || '当前操作下无需处理');
      return;
    }
    let route = cfg.route || '';
    if (!route) {
      this.msg.info(cfg.note || '暂无对应页面');
      return;
    }
    route = route.replace(':namespace', encodeURIComponent(this.namespace)).replace(':name', encodeURIComponent(this.name));
    this.router.navigateByUrl(route);
  }

  navigateAfter(): void {
    this.router.navigate(['/clusters', this.namespace, this.name]);
  }

  goBackToCluster(): void {
    this.router.navigate(['/clusters', this.namespace, this.name]);
  }

  trackByIdx(i: number): number { return i; }
  trackByVersion(i: number, v: { version: string }): string { return v?.version; }
  vLabel(v: { version: string; recommended?: boolean }): string { return v ? `${v.version}${v.recommended ? ' (推荐)' : ''}` : ''; }
}


