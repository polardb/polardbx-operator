import { Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzTabsModule } from 'ng-zorro-antd/tabs';
import { NzGridModule } from 'ng-zorro-antd/grid';
import { NzDividerModule } from 'ng-zorro-antd/divider';
import { NzStepsModule } from 'ng-zorro-antd/steps';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

@Component({
  selector: 'app-helm-values-helper',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzFormModule,
    NzInputModule,
    NzButtonModule,
    NzIconModule,
    NzSwitchModule,
    NzTabsModule,
    NzGridModule,
    NzDividerModule,
    NzStepsModule,
    NzToolTipModule
  ],
  template: `
    <div class="helper">
      <div class="page-header">
        <div class="header-content">
          <h1 class="page-title">
            <i nz-icon nzType="tool" class="page-icon"></i>
            Helm 配置助手
          </h1>
          <p class="page-description">生成标准化 values.yaml 与安装命令，不修改官方 Chart 结构</p>
        </div>
      </div>

      <div class="page-content">
        <div class="configuration-wrapper">
          <!-- 配置步骤指引 -->
          <nz-card class="steps-card" nzTitle="配置步骤">
            <nz-steps [nzCurrent]="0" nzSize="small">
              <nz-step nzTitle="镜像配置" nzDescription="仓库和版本"></nz-step>
              <nz-step nzTitle="存储路径" nzDescription="数据目录"></nz-step>
              <nz-step nzTitle="调度策略" nzDescription="节点选择"></nz-step>
              <nz-step nzTitle="生成文件" nzDescription="导出配置"></nz-step>
            </nz-steps>
          </nz-card>

          <!-- 配置表单 -->
          <div class="config-sections">
            <!-- 镜像配置 -->
            <nz-card class="config-card" nzTitle="镜像配置" [nzExtra]="imageExtra">
              <ng-template #imageExtra>
                <i nz-icon nzType="global" class="section-icon"></i>
              </ng-template>
              <form [formGroup]="form" nz-form nzLayout="vertical">
                <div nz-row nzGutter="16">
                  <div nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label nzRequired nz-tooltip nzTooltipTitle="镜像仓库地址，用于拉取 PolarDB-X 相关镜像">镜像仓库</nz-form-label>
                      <nz-form-control>
                        <input nz-input formControlName="imageRepo" placeholder="polardbx-opensource-registry.cn-beijing.cr.aliyuncs.com/polardbx" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  <div nz-col [nzSpan]="8">
                    <nz-form-item>
                      <nz-form-label nzRequired>镜像版本</nz-form-label>
                      <nz-form-control>
                        <input nz-input formControlName="imageTag" placeholder="v1.7.0" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  <div nz-col [nzSpan]="4">
                    <nz-form-item>
                      <nz-form-label>使用 latest</nz-form-label>
                      <nz-form-control>
                        <nz-switch formControlName="useLatestImage"></nz-switch>
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                </div>
                <div nz-row>
                  <div nz-col [nzSpan]="24">
                    <nz-form-item>
                      <nz-form-label nz-tooltip nzTooltipTitle="私有镜像仓库认证密钥，多个用逗号分隔">镜像拉取密钥</nz-form-label>
                      <nz-form-control>
                        <input nz-input formControlName="imagePullSecrets" placeholder="regcred,another-secret" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                </div>
              </form>
            </nz-card>

            <!-- 存储路径配置 -->
            <nz-card class="config-card" nzTitle="存储路径" [nzExtra]="storageExtra">
              <ng-template #storageExtra>
                <i nz-icon nzType="hdd" class="section-icon"></i>
              </ng-template>
              <form [formGroup]="form" nz-form nzLayout="vertical">
                <div nz-row nzGutter="16">
                  <div nz-col [nzSpan]="8">
                    <nz-form-item>
                      <nz-form-label nzRequired>数据路径</nz-form-label>
                      <nz-form-control nzExtra="node.volumes.data">
                        <input nz-input formControlName="dataPath" placeholder="/data" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  <div nz-col [nzSpan]="8">
                    <nz-form-item>
                      <nz-form-label nzRequired>日志路径</nz-form-label>
                      <nz-form-control nzExtra="node.volumes.log">
                        <input nz-input formControlName="logPath" placeholder="/data-log" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  <div nz-col [nzSpan]="8">
                    <nz-form-item>
                      <nz-form-label nzRequired>文件流路径</nz-form-label>
                      <nz-form-control nzExtra="node.volumes.filestream">
                        <input nz-input formControlName="filestreamPath" placeholder="/filestream" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                </div>
              </form>
            </nz-card>

            <!-- 调度策略配置 -->
            <nz-card class="config-card" nzTitle="调度策略" [nzExtra]="scheduleExtra">
              <ng-template #scheduleExtra>
                <i nz-icon nzType="cluster" class="section-icon"></i>
              </ng-template>
              <form [formGroup]="form" nz-form nzLayout="vertical">
                <div nz-row nzGutter="16">
                  <div nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label nz-tooltip nzTooltipTitle="节点选择器，格式: key=value，多项用逗号分隔">节点选择器</nz-form-label>
                      <nz-form-control>
                        <input nz-input formControlName="nodeSelector" placeholder="kubernetes.io/os=linux" />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                  <div nz-col [nzSpan]="12">
                    <nz-form-item>
                      <nz-form-label nz-tooltip nzTooltipTitle="容忍度配置，JSON 数组格式">容忍度</nz-form-label>
                      <nz-form-control>
                        <input nz-input formControlName="tolerations" placeholder='[{"key":"node-role.kubernetes.io/master","effect":"NoSchedule"}]' />
                      </nz-form-control>
                    </nz-form-item>
                  </div>
                </div>
              </form>
            </nz-card>
          </div>

          <!-- 结果预览 -->
          <nz-card class="preview-card" nzTitle="配置结果">
            <nz-tabset class="preview-tabs">
              <nz-tab nzTitle="values.yaml 预览">
                <div class="code-section">
                  <pre class="code">{{ valuesYaml }}</pre>
                  <div class="code-actions">
                    <button nz-button nzType="default" nzGhost (click)="copy(valuesYaml)">
                      <i nz-icon nzType="copy"></i>
                      复制 YAML
                    </button>
                    <button nz-button nzType="primary" (click)="downloadYaml()">
                      <i nz-icon nzType="download"></i>
                      下载 values.yaml
                    </button>
                  </div>
                </div>
              </nz-tab>
              <nz-tab nzTitle="Helm 命令">
                <div class="code-section">
                  <pre class="code">{{ helmCmd }}</pre>
                  <div class="code-actions">
                    <button nz-button nzType="default" nzGhost (click)="copy(helmCmd)">
                      <i nz-icon nzType="copy"></i>
                      复制命令
                    </button>
                  </div>
                </div>
              </nz-tab>
            </nz-tabset>
          </nz-card>

          <!-- 操作按钮 -->
          <div class="action-bar">
            <button nz-button nzSize="large" (click)="reset()">
              <i nz-icon nzType="reload"></i>
              重置配置
            </button>
            <button nz-button nzType="primary" nzSize="large" (click)="saveDefaults()">
              <i nz-icon nzType="save"></i>
              保存为默认
            </button>
          </div>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .helper {
      padding: 16px;
      background: #ffffff;
    }
    
    .page-header {
      margin-bottom: 16px;
    }
    
    .header-content {
      max-width: 1120px;
      margin: 0 auto;
    }
    
    .page-title {
      color: rgba(0, 0, 0, 0.87);
      font-size: 18px;
      font-weight: 500;
      margin: 0 0 4px 0;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    
    .page-icon {
      font-size: 20px;
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
    
    .configuration-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .steps-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    .config-sections {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    
    .config-card, .preview-card {
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
      overflow: hidden;
    }
    
    .section-icon {
      font-size: 16px;
      color: #1890ff;
    }
    
    .code-section {
      position: relative;
    }
    
    .code {
      background: #f5f5f5;
      color: #333;
      padding: 16px;
      border-radius: 6px;
      overflow-x: auto;
      max-height: 400px;
      font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
      font-size: 13px;
      line-height: 1.4;
      margin-bottom: 16px;
      border: 1px solid #e0e0e0;
    }
    
    .code-actions {
      display: flex;
      gap: 8px;
      justify-content: flex-end;
    }
    
    .action-bar {
      display: flex;
      justify-content: center;
      gap: 16px;
      padding: 16px;
      background: #fff;
      border-radius: 8px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border: 1px solid #e0e0e0;
    }
    
    /* 响应式设计 */
    @media (max-width: 768px) {
      .helper {
        padding: 16px;
      }
      
      .action-bar {
        flex-direction: column;
        align-items: center;
      }
      
      .action-bar button {
        width: 100%;
        max-width: 200px;
      }
      
      .code-actions {
        flex-direction: column;
      }
      
      .code-actions button {
        width: 100%;
      }
    }
  `]
})
export class HelmValuesHelperComponent implements OnInit {
  private fb = inject(FormBuilder);
  private msg = inject(NzMessageService);

  form: FormGroup = this.fb.group({
    imageRepo: ['polardbx-opensource-registry.cn-beijing.cr.aliyuncs.com/polardbx', [Validators.required]],
    imageTag: ['v1.7.0', [Validators.required]],
    useLatestImage: [false],
    dataPath: ['/data', [Validators.required]],
    logPath: ['/data-log', [Validators.required]],
    filestreamPath: ['/filestream', [Validators.required]],
    imagePullSecrets: [''],
    nodeSelector: ['kubernetes.io/os=linux'],
    tolerations: ['']
  });

  valuesYaml = '';
  helmCmd = '';

  ngOnInit(): void {
    this.loadDefaults();
    this.rebuildOutputs();
    this.form.valueChanges.subscribe(() => this.rebuildOutputs());
  }

  private loadDefaults(): void {
    try {
      const raw = localStorage.getItem('helmValuesHelper');
      if (raw) {
        const v = JSON.parse(raw);
        this.form.patchValue(v, { emitEvent: false });
      }
    } catch {}
  }

  saveDefaults(): void {
    localStorage.setItem('helmValuesHelper', JSON.stringify(this.form.value || {}));
    this.msg.success('默认值已保存');
  }

  reset(): void {
    localStorage.removeItem('helmValuesHelper');
    this.form.reset({
      imageRepo: 'polardbx-opensource-registry.cn-beijing.cr.aliyuncs.com/polardbx',
      imageTag: 'v1.7.0',
      useLatestImage: false,
      dataPath: '/data',
      logPath: '/data-log',
      filestreamPath: '/filestream',
      imagePullSecrets: '',
      nodeSelector: 'kubernetes.io/os=linux',
      tolerations: ''
    });
    this.rebuildOutputs();
  }

  private parseNodeSelector(input: string): Record<string,string> {
    const out: Record<string,string> = {};
    (input || '').split(',').map(s => s.trim()).filter(Boolean).forEach(pair => {
      const idx = pair.indexOf('=');
      if (idx > 0) out[pair.slice(0, idx).trim()] = pair.slice(idx+1).trim();
    });
    return out;
  }

  private rebuildOutputs(): void {
    const v = this.form.value as any;
    const pullSecrets = (v.imagePullSecrets || '').split(',').map((s:string)=>s.trim()).filter((s:string)=>!!s);
    const ns = this.parseNodeSelector(v.nodeSelector || '');
    let tolerations: any[] = [];
    try {
      tolerations = v.tolerations ? JSON.parse(v.tolerations) : [];
    } catch { tolerations = []; }

    // values.yaml
    const lines: string[] = [];
    lines.push('# 标准化 values 示例：不修改官方 Chart，作为 -f 注入');
    lines.push(`imageRepo: ${v.imageRepo || ''}`);
    lines.push(`imageTag: ${v.imageTag || ''}`);
    lines.push(`useLatestImage: ${!!v.useLatestImage}`);
    if (pullSecrets.length) {
      lines.push('imagePullSecrets:');
      pullSecrets.forEach((n:string)=>{ lines.push(`  - name: ${n}`); });
    } else {
      lines.push('imagePullSecrets: []');
    }
    lines.push('node:');
    lines.push('  volumes:');
    lines.push(`    data: ${v.dataPath || '/data'}`);
    lines.push(`    log: ${v.logPath || '/data-log'}`);
    lines.push(`    filestream: ${v.filestreamPath || '/filestream'}`);
    lines.push('controllerManager:');
    if (Object.keys(ns).length) {
      lines.push('  nodeSelector:');
      Object.entries(ns).forEach(([k,val])=> lines.push(`    ${k}: ${val}`));
    } else {
      lines.push('  nodeSelector: {}');
    }
    if (Array.isArray(tolerations) && tolerations.length) {
      lines.push('  tolerations:');
      tolerations.forEach((t:any)=>{
        lines.push('    - ' + this.yamlInline(t));
      });
    } else {
      lines.push('  tolerations: {}');
    }

    this.valuesYaml = lines.join('\n');

    // helm command
    const nsArg = 'polardbx-operator-system';
    this.helmCmd = `helm upgrade --install polardbx-operator charts/polardbx-operator -n ${nsArg} --create-namespace -f values.yaml`;
  }

  private yamlInline(obj: any): string {
    if (!obj || typeof obj !== 'object') return `${obj}`;
    const parts: string[] = [];
    Object.entries(obj).forEach(([k,v])=>{
      const val = typeof v === 'string' ? JSON.stringify(v) : (v as any);
      parts.push(`${k}: ${val}`);
    });
    return parts.join(', ');
  }

  copy(text: string): void {
    navigator.clipboard.writeText(text || '').then(()=> this.msg.success('已复制到剪贴板'));
  }

  downloadYaml(): void {
    const blob = new Blob([this.valuesYaml || ''], { type: 'text/yaml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'values.yaml';
    a.click();
    URL.revokeObjectURL(url);
  }
}

