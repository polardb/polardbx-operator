import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, FormGroup } from '@angular/forms';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzFormModule } from 'ng-zorro-antd/form';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzInputNumberModule } from 'ng-zorro-antd/input-number';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzDatePickerModule } from 'ng-zorro-antd/date-picker';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzSwitchModule } from 'ng-zorro-antd/switch';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { Router, ActivatedRoute } from '@angular/router';
import { ApiService } from '../../services/api.service';
import { LogPreset, LogsQueryRequest, NormalizedResponse } from '../../models/logs.model';
import { LoadingService, LoadingKeys } from '../../services/loading.service';

@Component({
  selector: 'app-logs-query',
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    NzCardModule,
    NzFormModule,
    NzInputModule,
    NzInputNumberModule,
    NzSelectModule,
    NzButtonModule,
    NzDatePickerModule,
    NzSpinModule,
    NzSwitchModule,
    NzTableModule,
    NzAlertModule,
    NzEmptyModule,
    NzIconModule
  ],
  template: `
    <div class="logs-query">
      <!-- 查询表单区域 -->
      <nz-card class="query-card">
        <nz-alert 
          nzType="info" 
          nzShowIcon 
          nzMessage="Elasticsearch 主机由后端安全白名单控制"
          class="security-notice">
        </nz-alert>

        <form [formGroup]="form" class="query-form">
          <div class="form-row">
            <nz-form-item>
              <nz-form-label>预设</nz-form-label>
              <nz-form-control>
                <nz-select 
                  formControlName="preset" 
                  nzAllowClear 
                  nzPlaceHolder="选择预设" 
                  (ngModelChange)="onPresetChange($event)">
                  <nz-option *ngFor="let p of presets" [nzValue]="p.indexPattern" [nzLabel]="p.indexPattern"></nz-option>
                </nz-select>
              </nz-form-control>
            </nz-form-item>
            
            <nz-form-item>
              <nz-form-label>索引</nz-form-label>
              <nz-form-control>
                <input nz-input formControlName="index" placeholder="logs-*" />
              </nz-form-control>
            </nz-form-item>
          </div>

          <div class="form-row">
            <nz-form-item>
              <nz-form-label>时间范围</nz-form-label>
              <nz-form-control>
                <nz-range-picker 
                  formControlName="range" 
                  [nzRanges]="quickRanges">
                </nz-range-picker>
              </nz-form-control>
            </nz-form-item>
            
            <nz-form-item>
              <nz-form-label>数量</nz-form-label>
              <nz-form-control>
                <nz-input-number 
                  formControlName="size" 
                  [nzMin]="1" 
                  [nzMax]="1000" 
                  nzPlaceHolder="50">
                </nz-input-number>
              </nz-form-control>
            </nz-form-item>
          </div>

          <div class="form-row">
            <nz-form-item>
              <nz-form-label>Facets 字段</nz-form-label>
              <nz-form-control>
                <nz-select 
                  formControlName="facetFields" 
                  nzMode="multiple" 
                  nzPlaceHolder="选择统计字段"
                  [nzMaxTagCount]="3">
                  <nz-option *ngFor="let f of facetOptions" [nzValue]="f" [nzLabel]="f"></nz-option>
                </nz-select>
              </nz-form-control>
            </nz-form-item>
          </div>

          <div class="form-row switches">
            <nz-form-item>
              <nz-form-label>归一化</nz-form-label>
              <nz-form-control>
                <nz-switch formControlName="normalize"></nz-switch>
              </nz-form-control>
            </nz-form-item>
            
            <nz-form-item>
              <nz-form-label>时间直方图</nz-form-label>
              <nz-form-control>
                <nz-switch formControlName="histogramEnabled"></nz-switch>
              </nz-form-control>
            </nz-form-item>
            
            <nz-form-item *ngIf="form.get('histogramEnabled')?.value">
              <nz-form-label>间隔</nz-form-label>
              <nz-form-control>
                <nz-select formControlName="histogramInterval" nzPlaceHolder="自动">
                  <nz-option *ngFor="let iv of histogramIntervals" [nzValue]="iv" [nzLabel]="iv"></nz-option>
                </nz-select>
              </nz-form-control>
            </nz-form-item>
          </div>

          <div class="form-actions">
            <button 
              nz-button 
              nzType="primary" 
              (click)="onSearch()" 
              [nzLoading]="isLoading('LOGS_QUERY')">
              <i nz-icon nzType="search"></i>
              查询
            </button>
            <button 
              nz-button 
              nzType="default"
              (click)="form.reset()">
              重置
            </button>
          </div>
        </form>
      </nz-card>

      <!-- 结果展示区域 -->
      <nz-card class="results-card" *ngIf="results" nzTitle="查询结果">
        <div class="results-meta">
          <span>共 {{ results.total || 0 }} 条记录</span>
          <span *ngIf="results.took">用时 {{ results.took }}ms</span>
        </div>

        <nz-spin [nzSpinning]="isLoading('LOGS_QUERY')">
          <!-- Facets 统计 -->
          <div *ngIf="results.facets && objectKeys(results.facets).length > 0" class="facets-section">
            <h4>统计信息</h4>
            <div class="facets-grid">
              <nz-card *ngFor="let facetName of objectKeys(results.facets)" [nzTitle]="facetName" nzSize="small">
                <div *ngFor="let item of results.facets[facetName]?.slice(0, 5)" class="facet-item">
                  <span>{{ item.key }}</span>
                  <span>{{ item.count }}</span>
                </div>
              </nz-card>
            </div>
          </div>

          <!-- 时间分布 -->
          <div *ngIf="results.histogram && results.histogram.length > 0" class="histogram-section">
            <h4>时间分布</h4>
            <div class="histogram-simple">
              <div *ngFor="let h of results.histogram" class="hist-item">
                <span>{{ formatHistogramTime(h.key) }}</span>
                <span>{{ h.count }}</span>
              </div>
            </div>
          </div>

          <!-- 日志表格 -->
          <div class="logs-section">
            <h4>日志记录</h4>
            <nz-table 
              [nzData]="results.items || []" 
              [nzShowPagination]="false" 
              nzSize="small">
              <thead>
                <tr>
                  <th *ngFor="let col of itemColumns">{{ col }}</th>
                </tr>
              </thead>
              <tbody>
                <tr *ngFor="let item of results.items">
                  <td *ngFor="let col of itemColumns">
                    {{ formatLogValue(item[col]) }}
                  </td>
                </tr>
              </tbody>
            </nz-table>
            
            <nz-empty *ngIf="!results.items || results.items.length === 0" 
                      nzNotFoundContent="未找到匹配的日志">
            </nz-empty>
          </div>
        </nz-spin>
      </nz-card>
    </div>
  `,
  styles: [`
    .logs-query {
      .query-card,
      .results-card {
        margin-bottom: 16px;
      }

      .security-notice {
        margin-bottom: 16px;
      }

      .query-form {
        .form-row {
          display: flex;
          gap: 16px;
          margin-bottom: 16px;
          flex-wrap: wrap;

          nz-form-item {
            flex: 1;
            min-width: 200px;
            margin-bottom: 0;
          }

          &.switches {
            nz-form-item {
              flex: 0 0 auto;
              min-width: 120px;
            }
          }
        }

        .form-actions {
          display: flex;
          gap: 8px;
          padding-top: 16px;
          border-top: 1px solid #f0f0f0;
        }
      }

      .results-meta {
        display: flex;
        gap: 16px;
        margin-bottom: 16px;
        font-size: 14px;
        color: #666;
      }

      .facets-section,
      .histogram-section,
      .logs-section {
        margin-bottom: 24px;

        h4 {
          margin: 0 0 12px 0;
          font-size: 16px;
          font-weight: 600;
        }
      }

      .facets-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 16px;

        .facet-item {
          display: flex;
          justify-content: space-between;
          padding: 4px 0;
          border-bottom: 1px solid #f0f0f0;

          &:last-child {
            border-bottom: none;
          }
        }
      }

      .histogram-simple {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(100px, 1fr));
        gap: 8px;

        .hist-item {
          text-align: center;
          padding: 8px;
          background: #f6f8fa;
          border-radius: 4px;
          font-size: 12px;

          span:first-child {
            display: block;
            font-weight: 500;
          }

          span:last-child {
            display: block;
            color: #1890ff;
            font-weight: 600;
          }
        }
      }
    }

    /* 响应式设计 */
    @media (max-width: 768px) {
      .logs-query .query-form .form-row {
        flex-direction: column;

        nz-form-item {
          min-width: 100%;
        }
      }

      .facets-grid {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class LogsQueryComponent implements OnInit {
  form: FormGroup;
  presets: LogPreset[] = [];
  facetOptions: string[] = [];
  histogramIntervals: string[] = ['30s','1m','5m','10m','30m','1h','3h','6h','12h','1d'];
  quickRanges: Record<string, Date[]> = {};
  results: NormalizedResponse | null = null;
  itemColumns: string[] = [];

  constructor(
    private fb: FormBuilder,
    private api: ApiService,
    private loading: LoadingService,
    private message: NzMessageService,
    private router: Router,
    private route: ActivatedRoute
  ) {
    this.form = this.fb.group({
      preset: [null],
      index: [''],
      range: [null],
      size: [50],
      facetFields: [[]],
      normalize: [true],
      histogramEnabled: [true],
      histogramInterval: [null]
    });
    this.quickRanges = this.buildQuickRanges();
  }

  ngOnInit(): void {
    this.api.getLogPresets().subscribe({
      next: (res) => { this.presets = res?.items || []; this.applyQueryParams(); },
      error: () => { this.message.error('加载预设失败'); this.applyQueryParams(); }
    });
  }

  private buildQuickRanges(): Record<string, Date[]> {
    const now = new Date();
    const minus = (ms: number) => new Date(now.getTime() - ms);
    return {
      '近15分钟': [minus(15 * 60 * 1000), now],
      '近1小时': [minus(60 * 60 * 1000), now],
      '近6小时': [minus(6 * 60 * 60 * 1000), now],
      '近24小时': [minus(24 * 60 * 60 * 1000), now]
    };
  }

  private applyQueryParams(): void {
    const qp = this.route.snapshot.queryParamMap;
    const preset = qp.get('preset');
    const index = qp.get('index');
    const size = qp.get('size');
    const from = qp.get('from');
    const to = qp.get('to');
    const facets = qp.get('facets');
    const normalize = qp.get('normalize');
    const hi = qp.get('hi'); // histogram interval
    const he = qp.get('he'); // histogram enabled

    const patch: any = {};
    if (preset) patch.preset = preset;
    if (index) patch.index = index;
    if (size) patch.size = Number(size) || 50;
    if (from && to) {
      const d1 = new Date(from);
      const d2 = new Date(to);
      if (!isNaN(d1.getTime()) && !isNaN(d2.getTime())) patch.range = [d1, d2];
    }
    if (facets) patch.facetFields = facets.split(',').filter(Boolean);
    if (normalize !== null) patch.normalize = normalize === '1' || normalize === 'true';
    if (he !== null) patch.histogramEnabled = he === '1' || he === 'true';
    if (hi) patch.histogramInterval = hi;

    this.form.patchValue(patch);
    if (preset) this.onPresetChange(preset);

    // auto search when index provided
    if (index) this.onSearch();
  }

  private updateUrl(): void {
    const v = this.form.value;
    const qp: any = {
      preset: v.preset || undefined,
      index: v.index || undefined,
      size: v.size || undefined,
      from: (v.range && v.range[0]?.toISOString?.()) || undefined,
      to: (v.range && v.range[1]?.toISOString?.()) || undefined,
      facets: (Array.isArray(v.facetFields) && v.facetFields.length) ? v.facetFields.join(',') : undefined,
      normalize: v.normalize ? '1' : '0',
      hi: v.histogramInterval || undefined,
      he: v.histogramEnabled ? '1' : '0'
    };
    this.router.navigate([], { relativeTo: this.route, queryParams: qp, queryParamsHandling: 'merge' });
  }

  isLoading(key: string): boolean { return this.loading.isLoading(key); }

  onPresetChange(pattern: string | null): void {
    const preset = this.presets.find(p => p.indexPattern === pattern) || null;
    this.facetOptions = preset?.facets || [];
    this.histogramIntervals = preset?.histogram?.intervals || ['30s','1m','5m','10m','30m','1h','3h','6h','12h','1d'];
    const defaultFacetSelection = (preset?.facets || []).slice(0, Math.min(3, (preset?.facets || []).length));
    this.form.patchValue({ index: pattern || '', facetFields: defaultFacetSelection, histogramInterval: null });
  }

  private deriveItemColumns(items: any[]): string[] {
    if (!Array.isArray(items) || items.length === 0) return [];
    const keys = Object.keys(items[0] || {});
    return keys.slice(0, Math.min(keys.length, 6));
  }

  objectKeys(obj: any): string[] { return Object.keys(obj || {}); }

  onSearch(): void {
    const v = this.form.value;
    if (!v.index || String(v.index).trim() === '') {
      this.message.warning('请先选择预设或填写索引');
      return;
    }
    const req: LogsQueryRequest = {
      index: v.index || '',
      size: Number(v.size) || 50,
      normalize: !!v.normalize
    };
    if (v.range && Array.isArray(v.range) && v.range.length === 2) {
      const [start, end] = v.range;
      req.timeRange = {
        field: '@timestamp',
        from: start?.toISOString?.() || start,
        to: end?.toISOString?.() || end
      };
    }
    const facetFields: string[] = Array.isArray(v.facetFields) ? v.facetFields : [];
    if (facetFields.length > 0) {
      req.facets = facetFields.map(f => ({ name: this.toFacetName(f), field: f, size: 10, order: 'count' }));
    }
    if (v.histogramEnabled) {
      req.histogram = { name: 'by_time', field: '@timestamp', interval: (v.histogramInterval || undefined) } as any;
    }

    this.updateUrl();
    this.api.queryLogs(req).subscribe({
      next: (res: any) => {
        this.results = res;
        const items = (res && res.items) ? res.items : [];
        this.itemColumns = this.deriveItemColumns(items);
      },
      error: () => { this.message.error('查询失败'); }
    });
  }

  private toFacetName(field: string): string {
    const base = field.replace(/\.keyword$/, '')
      .split('.')
      .filter(Boolean)
      .slice(-1)[0] || 'field';
    return `by_${base}`;
  }

  getMaxFacetCount(facetItems: any[]): number {
    if (!Array.isArray(facetItems) || facetItems.length === 0) return 1;
    return Math.max(...facetItems.map(item => item.count || 0));
  }

  getMaxHistogramCount(): number {
    if (!this.results?.histogram || this.results.histogram.length === 0) return 1;
    return Math.max(...this.results.histogram.map(h => h.count || 0));
  }

  formatHistogramTime(key: string): string {
    try {
      const date = new Date(key);
      return date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    } catch {
      return key;
    }
  }

  formatLogValue(value: any): string {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }
}