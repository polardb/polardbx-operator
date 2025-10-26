import { ChangeDetectorRef, Component, OnInit, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient, HttpClientModule } from '@angular/common/http';
import { Router } from '@angular/router';
import { forkJoin, of, lastValueFrom } from 'rxjs';
import { catchError, finalize } from 'rxjs/operators';

import { NzTableModule } from 'ng-zorro-antd/table';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzTypographyModule } from 'ng-zorro-antd/typography';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import { DashboardTemplateManifest, DashboardTemplateMeta, GrafanaTemplateDetail, GrafanaTemplateSummary } from '../../models/monitoring-dashboard.model';
import { YamlPreviewComponent } from '../yaml-preview/yaml-preview.component';

interface TemplateViewModel {
  name: string;
  title: string;
  description: string;
  tags: string[];
  category?: string;
  chartFile: string;
  source?: string;
  size?: number;
  updatedAt?: string;
  available: boolean;
  docLink?: string;
  recommended?: boolean;
  quickActions?: string[];
}

interface TemplateStats {
  total: number;
  available: number;
  recommended: number;
  missing: number;
}

@Component({
  selector: 'app-monitoring-dashboard-templates',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    HttpClientModule,
    NzTableModule,
    NzButtonModule,
    NzTagModule,
    NzToolTipModule,
    NzIconModule,
    NzDrawerModule,
    NzAlertModule,
    NzCardModule,
    NzSpinModule,
    NzEmptyModule,
    NzInputModule,
    NzSelectModule,
    NzCheckboxModule,
    NzTypographyModule,
    YamlPreviewComponent
  ],
  template: `
    <div class="dashboard-templates" *ngIf="initialized">
      <nz-alert
        nzType="info"
        nzShowIcon
        nzMessage="Grafana 仪表板模板"
        [nzDescription]="directory
          ? ('模板来源：' + directory + '，与官方 Helm Chart 同步。导入时将写入 polardbx-grafana-dashboards ConfigMap。')
          : '模板来源于 charts/polardbx-monitor/dashboard，导入时将写入 polardbx-grafana-dashboards ConfigMap。'">
      </nz-alert>

      <div class="filters">
        <div class="left">
          <nz-select
            nzSize="large"
            style="width: 200px;"
            [(ngModel)]="selectedCategory"
            (ngModelChange)="applyFilters()"
            nzPlaceHolder="分类筛选">
            <nz-option nzValue="all" nzLabel="所有分类"></nz-option>
            <nz-option
              *ngFor="let cat of categories"
              [nzValue]="cat"
              [nzLabel]="cat">
            </nz-option>
          </nz-select>
          <input
            nz-input
            nzSize="large"
            placeholder="搜索模板名称 / 描述 / 标签"
            [(ngModel)]="keyword"
            (ngModelChange)="applyFilters()"
          />
          <label nz-checkbox [(ngModel)]="onlyAvailable" (ngModelChange)="applyFilters()">
            仅显示可导入模板
          </label>
        </div>
        <div class="right">
          <button nz-button nzType="default" nzSize="large" (click)="reload()" [nzLoading]="loading">
            <i nz-icon nzType="reload"></i>
            刷新
          </button>
        </div>
      </div>

      <div class="stats" *ngIf="stats.total">
        <span>模板总数：<strong>{{ stats.total }}</strong></span>
        <span>可用：<strong>{{ stats.available }}</strong></span>
        <span>推荐：<strong>{{ stats.recommended }}</strong></span>
        <span *ngIf="stats.missing">待同步：<strong>{{ stats.missing }}</strong></span>
      </div>

      <nz-table
        #table
        class="template-table"
        [nzData]="filteredTemplates"
        nzSize="middle"
        [nzLoading]="loading"
        [(nzPageIndex)]="pageIndex"
        [(nzPageSize)]="pageSize"
        [nzPageSizeOptions]="pageSizeOptions"
        [nzShowSizeChanger]="true"
        [nzTotal]="filteredTemplates.length"
        (nzPageIndexChange)="onPageIndexChange($event)"
        (nzPageSizeChange)="onPageSizeChange($event)">
        <thead>
          <tr>
            <th style="width: 32%;">模板</th>
            <th style="width: 18%;">标签</th>
            <th style="width: 28%;">来源</th>
            <th style="width: 22%; text-align: right;">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr *ngFor="let item of table.data">
            <td>
              <div class="title-row">
                <span class="title">{{ item.title }}</span>
                <nz-tag *ngIf="item.recommended" nzColor="processing">推荐</nz-tag>
                <nz-tag *ngIf="!item.available" nzColor="default">待同步</nz-tag>
              </div>
              <div class="meta-row">
                <span class="meta">标识：{{ item.name }}</span>
                <span class="meta" *ngIf="item.category">分类：{{ item.category }}</span>
              </div>
              <p class="description">{{ item.description }}</p>
            </td>
            <td>
              <div class="tag-list">
                <nz-tag *ngFor="let tag of item.tags" nzColor="blue">{{ tag }}</nz-tag>
                <span *ngIf="!item.tags.length" class="placeholder">无标签</span>
              </div>
            </td>
            <td>
              <div class="source">{{ item.chartFile }}</div>
              <div class="extra" *ngIf="item.source">{{ item.source }}</div>
              <div class="extra" *ngIf="item.size">大小：{{ item.size / 1024 | number:'1.0-1' }} KB</div>
              <div class="extra" *ngIf="item.updatedAt">更新：{{ item.updatedAt | date:'yyyy-MM-dd HH:mm' }}</div>
            </td>
            <td class="actions">
              <button
                nz-button
                nzType="link"
                nzTooltipTitle="在侧边栏预览 JSON"
                nz-tooltip
                (click)="previewTemplate(item)"
                [disabled]="!item.available"
              >
                <i nz-icon nzType="eye"></i>
                预览
              </button>
              <button
                nz-button
                nzType="link"
                [disabled]="!item.available"
                (click)="importTemplate(item)"
                [nzLoading]="importingName === item.name"
              >
                <i nz-icon nzType="cloud-upload"></i>
                导入 Grafana
              </button>
              <a
                *ngIf="item.docLink"
                nz-button
                nzType="link"
                nzSize="default"
                [href]="item.docLink"
                target="_blank"
                rel="noopener"
              >
                <i nz-icon nzType="link"></i>
                官方说明
              </a>
            </td>
          </tr>
        </tbody>
      </nz-table>

      <nz-empty *ngIf="!loading && !filteredTemplates.length" nzNotFoundContent="暂无匹配的模板"></nz-empty>

      <nz-card nzTitle="Grafana 访问指引" class="hint-card">
        <p>
          Grafana 默认通过 <code>ClusterIP</code> 暴露，可在导入模板后通过如下方式访问：
        </p>
        <ul class="hint-list">
          <li class="hint-item">
            <span class="label">port-forward：</span>
            <div class="command-row">
              <pre class="command">{{ portForwardCommand }}</pre>
              <button
                nz-button
                nzType="default"
                nzSize="small"
                class="copy-btn"
                (click)="copyPortForward()"
                [nzLoading]="copyingPortForward">
                <i nz-icon nzType="copy"></i>
                复制
              </button>
            </div>
          </li>
          <li class="hint-item">
            <span class="label">NodePort：</span>
            <span class="description">若已修改为 NodePort 类型，可使用 <code>http://&lt;node-ip&gt;:&lt;node-port&gt;</code> 访问。</span>
          </li>
        </ul>
        <p class="info">导入的仪表板将保存在 ConfigMap <code>polardbx-grafana-dashboards</code> 中，并由 Grafana sidecar 自动加载。</p>
        <div class="hint-actions">
          <button nz-button nzType="primary" (click)="gotoGrafana()">
            <i nz-icon nzType="line-chart"></i>
            打开内置 Grafana 页面
          </button>
        </div>
      </nz-card>

      <nz-drawer
        [nzVisible]="drawerVisible"
        [nzTitle]="previewState.title"
        [nzWidth]="720"
        (nzOnClose)="closeDrawer()">
        <ng-container *nzDrawerContent>
          <app-yaml-preview
            [yamlContent]="previewState.content"
            [filename]="previewState.filename"
            [language]="'json'"
            [cardTitle]="'Dashboard JSON 预览'"
            [loading]="previewLoading"
            [readonly]="true"
            [usePlainRenderer]="false">
          </app-yaml-preview>
        </ng-container>
      </nz-drawer>
    </div>
  `,
  styles: [`
    .dashboard-templates {
      display: flex;
      flex-direction: column;
      gap: 16px;
      padding: 8px;
    }

    .filters {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }

    .filters .left {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }

    .filters .left label {
      margin: 0 8px;
      user-select: none;
    }

    .stats {
      display: flex;
      gap: 24px;
      font-size: 14px;
      color: rgba(0, 0, 0, 0.65);
    }

    .template-table ::ng-deep .ant-table-tbody > tr > td {
      vertical-align: top;
    }

    .title-row {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 4px;
    }

    .title {
      font-weight: 600;
      font-size: 16px;
      color: rgba(0, 0, 0, 0.85);
    }

    .meta-row {
      display: flex;
      gap: 16px;
      font-size: 12px;
      color: rgba(0, 0, 0, 0.45);
      margin-bottom: 6px;
    }

    .description {
      margin: 0;
      font-size: 13px;
      color: rgba(0, 0, 0, 0.65);
    }

    .tag-list {
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
    }

    .placeholder {
      color: rgba(0, 0, 0, 0.35);
    }

    .source {
      font-weight: 500;
      color: rgba(0, 0, 0, 0.75);
    }

    .extra {
      font-size: 12px;
      color: rgba(0, 0, 0, 0.45);
    }

    .actions {
      text-align: right;
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 4px;
    }

    .hint-card {
      margin-top: 8px;
    }

    .hint-list {
      list-style: none;
      padding: 0;
      margin: 12px 0;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    .hint-item .label {
      font-weight: 600;
      color: rgba(0, 0, 0, 0.75);
    }

    .hint-item .description {
      color: rgba(0, 0, 0, 0.65);
      line-height: 1.6;
    }

    .command-row {
      margin-top: 6px;
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }

    .command-row .command {
      margin: 0;
      padding: 8px 12px;
      border-radius: 6px;
      background: #0d1117;
      color: #f8f8f2;
      font-family: 'SFMono-Regular', 'Menlo', 'Monaco', monospace;
      font-size: 12px;
      flex: 1 1 auto;
      min-width: 260px;
      overflow-x: auto;
      white-space: pre;
    }

    .command-row .copy-btn {
      flex: 0 0 auto;
    }

    .hint-actions {
      margin-top: 12px;
    }

    @media (max-width: 960px) {
      .actions {
        flex-direction: row;
        justify-content: flex-end;
        flex-wrap: wrap;
      }
    }
  `]
})
export class MonitoringDashboardTemplatesComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly http = inject(HttpClient);
  private readonly message = inject(NzMessageService);
  private readonly loadingService = inject(LoadingService);
  private readonly router = inject(Router);
  private readonly cdr = inject(ChangeDetectorRef);

  initialized = false;
  loading = false;
  templates: TemplateViewModel[] = [];
  filteredTemplates: TemplateViewModel[] = [];
  categories: string[] = [];
  selectedCategory = 'all';
  keyword = '';
  onlyAvailable = true;
  stats: TemplateStats = { total: 0, available: 0, recommended: 0, missing: 0 };
  directory = '';

  drawerVisible = false;
  previewState = {
    title: '',
    filename: 'dashboard.json',
    content: ''
  };
  previewLoading = false;
  importingName: string | null = null;
  readonly portForwardCommand = 'kubectl port-forward svc/grafana -n polardbx-monitor 3000:3000';
  copyingPortForward = false;

  private cache = new Map<string, GrafanaTemplateDetail>();
  private showedFallbackNotice = false;
  pageIndex = 1;
  pageSize = 10;
  readonly pageSizeOptions = [10, 20, 50];

  ngOnInit(): void {
    console.log('[仪表板模板] 组件初始化');
    this.loadData();
  }

  reload(): void {
    this.loadData(true);
  }

  applyFilters(): void {
    this.pageIndex = 1;
    const keyword = this.keyword.trim().toLowerCase();
    this.filteredTemplates = this.templates.filter(template => {
      if (this.onlyAvailable && !template.available) {
        return false;
      }
      if (this.selectedCategory !== 'all' && template.category !== this.selectedCategory) {
        return false;
      }
      if (!keyword) {
        return true;
      }
      return [
        template.name,
        template.title,
        template.description,
        ...(template.tags || [])
      ].some(field => field?.toLowerCase().includes(keyword));
    });
  }

  onPageIndexChange(page: number): void {
    this.pageIndex = page;
  }

  onPageSizeChange(size: number): void {
    this.pageSize = size;
    this.pageIndex = 1;
  }

  async previewTemplate(template: TemplateViewModel): Promise<void> {
    if (!template.available) {
      this.message.warning('该模板尚未同步到后端目录');
      return;
    }
    const drawerTitle = `${template.title} (${template.name})`;
    const filename = template.chartFile || `${template.name}.json`;

    console.log('[预览] 开始预览模板:', {
      name: template.name,
      title: template.title,
      filename,
      available: template.available
    });

    this.previewLoading = true;
    this.drawerVisible = true;
    this.previewState = {
      title: drawerTitle,
      filename,
      content: ''
    };

    try {
      console.log('[预览] 正在获取模板详情...');
      const detail = await this.fetchTemplateDetail(template);
      console.log('[预览] 模板详情获取成功:', {
        name: detail.name,
        contentType: typeof detail.content,
        contentLength: detail.content ? JSON.stringify(detail.content).length : 0
      });

      const pretty = this.stringifyTemplate(detail.content);
      let fallback = '';
      if (detail?.content) {
        try {
          const raw = JSON.stringify(detail.content, null, 2);
          fallback = raw && raw.trim() ? raw : '';
        } catch (jsonErr) {
          console.warn('[预览] 无法格式化模板 JSON，使用字符串输出', jsonErr);
          fallback = String(detail.content);
        }
      }
      const normalized = (pretty && pretty.trim()) ? pretty : fallback;

      console.log('[预览] 内容格式化完成:', {
        prettyLength: pretty?.length || 0,
        fallbackLength: fallback?.length || 0,
        normalizedLength: normalized?.length || 0
      });

      if (!normalized) {
        console.warn('[预览] 模板内容为空');
        this.message.warning('模板内容为空或无法解析，请确认 JSON 文件有效。');
      }

      this.previewState = {
        title: drawerTitle,
        filename,
        content: normalized
      };

      console.log('[预览] 预览状态已更新:', {
        drawerVisible: this.drawerVisible,
        contentLength: this.previewState.content.length
      });
    } catch (err) {
      console.error('[预览] 加载 Grafana 模板失败:', err);
      this.previewState = {
        title: drawerTitle,
        filename,
        content: ''
      };
      this.message.error('加载模板内容失败');
    } finally {
      this.previewLoading = false;
      this.cdr.markForCheck();
      console.log('[预览] 预览流程结束, loading:', this.previewLoading);
    }
  }

  async importTemplate(template: TemplateViewModel): Promise<void> {
    if (!template.available) {
      this.message.warning('该模板尚未同步到后端目录');
      return;
    }

    this.importingName = template.name;

    try {
  const detail = await this.fetchTemplateDetail(template);
      const payload = {
        dashboards: {
          [template.chartFile || `${template.name}.json`]: JSON.stringify(detail.content, null, 2)
        },
        overwrite: true
      };
      await lastValueFrom(this.api.syncGrafanaDashboards(payload));
      this.message.success(`仪表板 ${template.title} 导入成功`);
    } catch (err: any) {
      console.error('导入 Grafana 仪表板失败:', err);
      const reason = err?.error?.error || err?.message || '未知错误';
      this.message.error(`导入失败：${reason}`);
    } finally {
      this.importingName = null;
      this.cdr.markForCheck();
    }
  }

  async copyPortForward(): Promise<void> {
    if (this.copyingPortForward) {
      return;
    }
    const command = this.portForwardCommand;
    if (typeof navigator === 'undefined' || !('clipboard' in navigator) || !navigator.clipboard) {
      this.message.warning('当前浏览器不支持自动复制，请手动复制命令');
      return;
    }

    this.copyingPortForward = true;
    try {
      await navigator.clipboard.writeText(command);
      this.message.success('port-forward 命令已复制到剪贴板');
    } catch (error) {
      console.error('复制 port-forward 命令失败:', error);
      this.message.error('复制失败，请手动复制命令');
    } finally {
      this.copyingPortForward = false;
    }
  }

  closeDrawer(): void {
    console.log('[预览] 关闭抽屉');
    this.drawerVisible = false;
    this.previewState = {
      title: '',
      filename: 'dashboard.json',
      content: ''
    };
    this.cdr.markForCheck();
  }

  gotoGrafana(): void {
    this.router.navigate(['/operations/monitoring/grafana']);
  }

  private loadData(force = false): void {
    if (this.loading) {
      return;
    }
    this.loading = true;
    this.loadingService.setLoading(LoadingKeys.GRAFANA_TEMPLATE_LIST, true);

    const manifest$ = this.http
      .get<DashboardTemplateManifest>('assets/monitoring/dashboard-templates.json')
      .pipe(catchError(err => {
        console.warn('加载模板清单失败，使用空清单:', err);
        return of({ templates: [] } as DashboardTemplateManifest);
      }));

    const summaries$ = this.api
      .listGrafanaTemplates()
      .pipe(catchError(err => {
        console.error('加载 Grafana 模板失败:', err);
        this.message.error('无法读取 Grafana 模板目录，请确认后端部署包含 charts 目录。');
        return of({ items: [] as GrafanaTemplateSummary[], directory: '' });
      }));

    forkJoin({ manifest: manifest$, summaries: summaries$ })
      .pipe(finalize(() => {
        this.loading = false;
        this.loadingService.setLoading(LoadingKeys.GRAFANA_TEMPLATE_LIST, false);
        this.initialized = true;
      }))
      .subscribe(({ manifest, summaries }) => {
        const merged = this.mergeTemplates(manifest.templates ?? [], summaries.items ?? []);
        this.templates = merged;
        this.directory = summaries.directory || '';
        this.categories = Array.from(new Set(
          merged
            .map(item => item.category)
            .filter((cat): cat is string => !!cat)
        ))
          .sort();

        this.updateStats();
        this.applyFilters();
      });
  }

  private mergeTemplates(manifest: DashboardTemplateMeta[], summaries: GrafanaTemplateSummary[]): TemplateViewModel[] {
    const manifestMap = new Map<string, DashboardTemplateMeta>();
    manifest.forEach(item => manifestMap.set(item.name, item));

    const merged: TemplateViewModel[] = summaries.map(summary => {
      const meta = manifestMap.get(summary.name);
      if (meta) {
        manifestMap.delete(summary.name);
      }
      const tags = new Set<string>();
      (summary.tags || []).forEach(tag => tags.add(tag));
      (meta?.tags || []).forEach(tag => tags.add(tag));
      return {
        name: summary.name,
        title: meta?.title || summary.title || summary.name,
        description: meta?.description || summary.description || '',
        tags: Array.from(tags),
        category: meta?.category,
        chartFile: meta?.chart || summary.file || `${summary.name}.json`,
        source: summary.source,
        size: summary.size,
        updatedAt: summary.updatedAt,
        available: true,
        docLink: meta?.docLink,
        recommended: Boolean(meta?.recommended),
        quickActions: meta?.quickActions || []
      };
    });

    manifestMap.forEach(meta => {
      merged.push({
        name: meta.name,
        title: meta.title,
        description: meta.description,
        tags: meta.tags || [],
        category: meta.category,
        chartFile: meta.chart || `${meta.name}.json`,
        available: false,
        docLink: meta.docLink,
        recommended: Boolean(meta.recommended),
        quickActions: meta.quickActions || []
      });
    });

    return merged.sort((a, b) => a.title.localeCompare(b.title));
  }

  private async fetchTemplateDetail(template: TemplateViewModel): Promise<GrafanaTemplateDetail> {
    const cacheKey = template.name;
    if (this.cache.has(cacheKey)) {
      console.log('[获取模板] 使用缓存:', cacheKey);
      return this.cache.get(cacheKey)!;
    }

    console.log('[获取模板] 从后端获取:', template.name);
    this.loadingService.setLoading(LoadingKeys.GRAFANA_TEMPLATE_DETAIL, true);
    try {
      const detail = await lastValueFrom(this.api.getGrafanaTemplate(template.name, { silent: true }));
      console.log('[获取模板] 后端返回成功:', {
        name: detail.name,
        hasContent: !!detail.content
      });
      this.cache.set(cacheKey, detail);
      return detail;
    } catch (error) {
      console.warn(`[获取模板] 从后端加载 Grafana 模板 ${template.name} 失败，尝试使用内置资源。`, error);
      const fallback = await this.loadTemplateFromAssets(template);
      if (fallback) {
        if (!this.showedFallbackNotice) {
          this.message.info('未连接到后端 Grafana 模板目录，已使用内置模板进行预览。');
          this.showedFallbackNotice = true;
        }
        console.log('[获取模板] 使用内置资源成功');
        this.cache.set(cacheKey, fallback);
        return fallback;
      }
      console.error('[获取模板] 内置资源也加载失败');
      throw error;
    } finally {
      this.loadingService.setLoading(LoadingKeys.GRAFANA_TEMPLATE_DETAIL, false);
    }
  }

  private async loadTemplateFromAssets(template: TemplateViewModel): Promise<GrafanaTemplateDetail | null> {
    const filename = template.chartFile || `${template.name}.json`;
    if (!filename) {
      return null;
    }

    const assetUrl = `/assets/monitoring/dashboards/${filename}`;
    try {
      const raw = await lastValueFrom(this.http.get(assetUrl, { responseType: 'text' }));
      if (!raw?.trim()) {
        return null;
      }

      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch (parseError) {
        console.error(`本地模板 ${filename} 解析失败:`, parseError);
        return null;
      }

      const detail: GrafanaTemplateDetail = {
        name: template.name,
        title: template.title,
        description: template.description,
        tags: template.tags,
        file: filename,
        source: template.source || 'charts/polardbx-monitor/dashboard',
        size: template.size ?? raw.length,
        updatedAt: template.updatedAt || new Date().toISOString(),
        content: parsed
      };
      return detail;
    } catch (assetError) {
      console.error(`本地模板 ${filename} 加载失败:`, assetError);
      return null;
    }
  }

  private updateStats(): void {
    const total = this.templates.length;
    const available = this.templates.filter(t => t.available).length;
    const recommended = this.templates.filter(t => t.recommended).length;
    const missing = total - available;
    this.stats = { total, available, recommended, missing };
  }

  private stringifyTemplate(content: unknown): string {
    if (typeof content === 'string') {
      const trimmed = content.trim();
      if (!trimmed) {
        return '';
      }
      try {
        const parsed = JSON.parse(trimmed);
        return JSON.stringify(parsed, null, 2);
      } catch {
        return trimmed;
      }
    }

    if (content && typeof content === 'object') {
      try {
        return JSON.stringify(content, null, 2);
      } catch (error) {
        console.warn('无法格式化模板内容，使用原始字符串:', error);
        return String(content);
      }
    }

    return '';
  }
}
