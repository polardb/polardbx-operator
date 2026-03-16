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
import {
  FacetBucket,
  LogFacetRequest,
  LogHistogramRequest,
  LogItem,
  LogPreset,
  LogsPresetList,
  LogsQueryRequest,
  NormalizedResponse
} from '../../models/logs.model';
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
    <div class="page-wrapper logs-query">
      <div class="page-header">
        <div class="title-block">
          <h2>
            <i nz-icon nzType="search" nzTheme="outline" class="page-icon"></i>
            日志查询
          </h2>
          <p>对 Elasticsearch 索引进行查询、分组统计与时间分布分析</p>
        </div>
      </div>

      <nz-alert
        nzType="info"
        nzShowIcon
        nzMessage="安全提示"
        nzDescription="Elasticsearch 主机由后端安全白名单控制，仅允许访问受信任的地址。"
        class="security-notice">
      </nz-alert>

      <div class="page-content">
        <!-- 查询表单区域 -->
        <nz-card class="query-card" nzTitle="查询条件">
          <form [formGroup]="form" class="query-form">
            <div class="form-row">
              <nz-form-item>
                <nz-form-label>预设</nz-form-label>
                <nz-form-control>
                  <nz-select
                    formControlName="preset"
                    nzAllowClear
                    nzSize="small"
                    nzPlaceHolder="选择预设（会自动填充索引）"
                    (ngModelChange)="onPresetChange($event)">
                    <nz-option *ngFor="let p of presets" [nzValue]="p.indexPattern" [nzLabel]="p.indexPattern"></nz-option>
                  </nz-select>
                </nz-form-control>
              </nz-form-item>

              <nz-form-item>
                <nz-form-label>索引</nz-form-label>
                <nz-form-control>
                  <input nz-input nzSize="small" formControlName="index" placeholder="例如：polardbx-logs-*" />
                </nz-form-control>
              </nz-form-item>
            </div>

            <div class="form-row">
              <nz-form-item>
                <nz-form-label>时间范围</nz-form-label>
                <nz-form-control>
                  <nz-range-picker nzSize="small" formControlName="range" [nzRanges]="quickRanges"></nz-range-picker>
                </nz-form-control>
              </nz-form-item>

              <nz-form-item>
                <nz-form-label>数量</nz-form-label>
                <nz-form-control>
                  <nz-input-number
                    formControlName="size"
                    [nzMin]="1"
                    [nzMax]="1000"
                    nzSize="small"
                    nzPlaceHolder="50">
                  </nz-input-number>
                </nz-form-control>
              </nz-form-item>
            </div>

            <div class="form-row">
              <nz-form-item>
                <nz-form-label>统计字段（Facets）</nz-form-label>
                <nz-form-control>
                  <nz-select
                    formControlName="facetFields"
                    nzMode="multiple"
                    nzSize="small"
                    nzPlaceHolder="选择统计字段（可多选）"
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
                  <nz-select formControlName="histogramInterval" nzSize="small" nzPlaceHolder="自动">
                    <nz-option *ngFor="let iv of histogramIntervals" [nzValue]="iv" [nzLabel]="iv"></nz-option>
                  </nz-select>
                </nz-form-control>
              </nz-form-item>
            </div>

            <div class="form-actions">
              <button nz-button nzType="primary" (click)="onSearch()" [nzLoading]="isLoading('LOGS_QUERY')">
                <i nz-icon nzType="search"></i>
                查询
              </button>
              <button nz-button nzType="default" (click)="resetForm()">
                重置
              </button>
            </div>
          </form>
        </nz-card>

        <!-- 初始引导（未查询时） -->
        <nz-card *ngIf="!results" class="empty-card" nzTitle="开始查询">
          <nz-empty nzNotFoundContent="请在上方填写索引与时间范围，然后点击“查询”">
            <ng-template #nzNotFoundFooter>
              <div class="empty-actions">
                <button nz-button nzType="primary" (click)="onSearch()">
                  <i nz-icon nzType="search"></i>
                  立即查询
                </button>
                <button nz-button nzType="default" (click)="resetForm()">恢复默认</button>
              </div>
              <div class="empty-tips">
                <div class="tip"><strong>推荐</strong>：优先选择“预设”，会自动填充索引与推荐的统计字段。</div>
                <div class="tip"><strong>说明</strong>：索引支持通配符（如 <code>polardbx-logs-*</code>）。</div>
              </div>
            </ng-template>
          </nz-empty>
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
              <nz-table [nzData]="logItems" [nzShowPagination]="false" nzSize="small">
                <thead>
                  <tr>
                    <th *ngFor="let col of itemColumns">{{ col }}</th>
                  </tr>
                </thead>
                <tbody>
                  <tr *ngFor="let item of logItems">
                    <td *ngFor="let col of itemColumns">
                      {{ formatLogValue(getItemValue(item, col)) }}
                    </td>
                  </tr>
                </tbody>
              </nz-table>

              <nz-empty *ngIf="logItems.length === 0" nzNotFoundContent="未找到匹配的日志"></nz-empty>
            </div>
          </nz-spin>
        </nz-card>
      </div>
    </div>
  `,
  styles: [`
    .page-wrapper {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 24px;
      min-height: 100%;
      background: transparent; /* 外层由 logs-hub 负责背景 */
    }

    .page-header {
      background: #fff;
      padding: 16px;
      border-radius: 10px;
      box-shadow: 0 2px 10px rgba(15, 23, 42, 0.04);
    }

    .title-block {
      h2 {
        margin: 0 0 8px;
        font-size: 22px;
        font-weight: 600;
        color: #1f1f1f;
        display: flex;
        align-items: center;
        gap: 10px;
      }

      p {
        margin: 0;
        color: #595959;
        line-height: 1.6;
      }
    }

    .page-icon {
      font-size: 22px;
      color: #1890ff;
    }

    .page-content {
      display: flex;
      flex-direction: column;
      gap: 16px;
      width: 100%;
    }

    .security-notice {
      border-radius: 10px;
    }

    .query-form {
      .form-row {
        display: flex;
        gap: 16px;
        margin-bottom: 16px;
        flex-wrap: wrap;

        nz-form-item {
          flex: 1;
          min-width: 240px;
          margin-bottom: 0;
        }

        &.switches {
          nz-form-item {
            flex: 0 0 auto;
            min-width: 140px;
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

    .empty-card {
      border-radius: 10px;

      .empty-actions {
        display: flex;
        gap: 8px;
        justify-content: center;
        margin-bottom: 12px;
      }

      .empty-tips {
        display: grid;
        gap: 6px;
        color: #8c8c8c;
        font-size: 12px;

        code {
          background: #f6f8fa;
          padding: 2px 6px;
          border-radius: 6px;
        }
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

    /* 响应式设计 */
    @media (max-width: 768px) {
      .query-form .form-row {
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
  histogramIntervals: string[] = [];
  quickRanges: Record<string, Date[]> = {};
  results: NormalizedResponse | null = null;
  logItems: LogItem[] = [];
  itemColumns: string[] = [];

  private readonly defaultHistogramIntervals: readonly string[] = ['30s','1m','5m','10m','30m','1h','3h','6h','12h','1d'];

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
    this.histogramIntervals = Array.from(this.defaultHistogramIntervals);
  }

  ngOnInit(): void {
    this.api.getLogPresets().subscribe({
      next: (res: LogsPresetList) => {
        this.presets = Array.from(res?.items ?? []);
        this.applyQueryParams();
      },
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
    this.router.navigate([], { relativeTo: this.route, queryParams: qp });
  }

  isLoading(key: string): boolean { return this.loading.isLoading(key); }

  onPresetChange(pattern: string | null): void {
    const preset = this.presets.find(p => p.indexPattern === pattern) || null;
    this.facetOptions = preset?.facets ? Array.from(preset.facets) : [];
    this.histogramIntervals = preset?.histogram?.intervals ? Array.from(preset.histogram.intervals) : Array.from(this.defaultHistogramIntervals);
    const defaultFacetSelection = preset?.facets ? preset.facets.slice(0, Math.min(3, preset.facets.length)) : [];
    this.form.patchValue({ index: pattern || '', facetFields: defaultFacetSelection, histogramInterval: null });
  }

  private deriveItemColumns(items: readonly LogItem[]): string[] {
    if (!Array.isArray(items) || items.length === 0) return [];
    const keys = Object.keys(items[0] || {});
    return keys.slice(0, Math.min(keys.length, 6));
  }

  objectKeys(obj: Record<string, unknown> | undefined | null): string[] {
    return Object.keys(obj || {});
  }

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
    const facetFields: string[] = Array.isArray(v.facetFields) ? [...v.facetFields] : [];
    if (facetFields.length > 0) {
      const facets: LogFacetRequest[] = facetFields.map(f => ({ name: this.toFacetName(f), field: f, size: 10, order: 'count' }));
      req.facets = facets;
    }
    if (v.histogramEnabled) {
      const histogram: LogHistogramRequest = {
        name: 'by_time',
        field: '@timestamp',
        interval: v.histogramInterval || undefined
      };
      req.histogram = histogram;
    }

    this.updateUrl();
    this.api.queryLogs(req).subscribe({
      next: (res: NormalizedResponse) => {
        this.results = res;
        this.logItems = Array.from(res?.items ?? []);
        this.itemColumns = this.deriveItemColumns(this.logItems);
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

  getMaxFacetCount(facetItems: readonly FacetBucket[] | undefined): number {
    if (!Array.isArray(facetItems) || facetItems.length === 0) return 1;
    return Math.max(...facetItems.map(item => item.count ?? 0));
  }

  getMaxHistogramCount(): number {
    if (!this.results?.histogram || this.results.histogram.length === 0) return 1;
    return Math.max(...this.results.histogram.map(h => h.count ?? 0));
  }

  formatHistogramTime(key: string): string {
    try {
      const date = new Date(key);
      return date.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    } catch {
      return key;
    }
  }

  getItemValue(item: LogItem, column: string): unknown {
    return item[column];
  }

  formatLogValue(value: unknown): string {
    if (value === null || value === undefined) return '';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  resetForm(): void {
    this.form.reset({
      preset: null,
      index: '',
      range: null,
      size: 50,
      facetFields: [],
      normalize: true,
      histogramEnabled: true,
      histogramInterval: null
    });
    this.results = null;
    this.logItems = [];
    this.itemColumns = [];
    this.facetOptions = [];
    this.histogramIntervals = Array.from(this.defaultHistogramIntervals);
    this.updateUrl();
  }
}