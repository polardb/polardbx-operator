import { Component, OnInit, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient, HttpClientModule } from '@angular/common/http';
import { forkJoin, of, lastValueFrom } from 'rxjs';
import { catchError, finalize } from 'rxjs/operators';

import { NzTableModule } from 'ng-zorro-antd/table';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzTagModule } from 'ng-zorro-antd/tag';
import { NzAlertModule } from 'ng-zorro-antd/alert';
import { NzCardModule } from 'ng-zorro-antd/card';
import { NzSpinModule } from 'ng-zorro-antd/spin';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzDrawerModule } from 'ng-zorro-antd/drawer';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzModalModule, NzModalService } from 'ng-zorro-antd/modal';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { NzTypographyModule } from 'ng-zorro-antd/typography';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzEmptyModule } from 'ng-zorro-antd/empty';

import { ApiService } from '../../services/api.service';
import { LoadingService, LoadingKeys } from '../../services/loading.service';
import {
  AlertRuleTemplateSummary,
  AlertRuleTemplateDetail,
  AlertRuleTemplateManifest,
  AlertRuleTemplateMeta,
  AlertRuleTemplateCategory,
  ApplyAlertRuleTemplatePayload,
  ApplyAlertRuleTemplateResponse
} from '../../models/monitoring-alert-template.model';
import { YamlPreviewComponent } from '../yaml-preview/yaml-preview.component';

interface TemplateViewModel {
  name: string;
  title: string;
  description: string;
  categoryId?: string;
  category?: string;
  severity?: string;
  tags: string[];
  recommended: boolean;
  available: boolean;
  source?: string;
  file?: string;
  groups: string[];
  summary?: AlertRuleTemplateSummary;
  manifest?: AlertRuleTemplateMeta;
  docLink?: string;
  defaultNamespace?: string;
}

interface TemplateStats {
  total: number;
  available: number;
  recommended: number;
  categories: Record<string, number>;
}

@Component({
  selector: 'app-monitoring-alert-rule-templates',
  standalone: true,
  imports: [
    CommonModule,
    FormsModule,
    HttpClientModule,
    NzTableModule,
    NzButtonModule,
    NzTagModule,
    NzAlertModule,
    NzCardModule,
    NzSpinModule,
    NzInputModule,
    NzSelectModule,
    NzIconModule,
    NzDrawerModule,
    NzModalModule,
    NzToolTipModule,
    NzTypographyModule,
    NzCheckboxModule,
    NzEmptyModule,
    YamlPreviewComponent
  ],
  templateUrl: './monitoring-alert-rule-templates.component.html',
  styleUrls: ['./monitoring-alert-rule-templates.component.scss']
})
export class MonitoringAlertRuleTemplatesComponent implements OnInit {
  private readonly api = inject(ApiService);
  private readonly http = inject(HttpClient);
  private readonly message = inject(NzMessageService);
  private readonly modal = inject(NzModalService);
  private readonly loadingService = inject(LoadingService);

  @ViewChild(YamlPreviewComponent)
  yamlPreview?: YamlPreviewComponent;

  initialized = false;
  loading = false;
  applying = false;
  templates: TemplateViewModel[] = [];
  filteredTemplates: TemplateViewModel[] = [];
  categories: { id: string; label: string }[] = [];
  severities: string[] = [];
  selectedCategory = 'all';
  selectedSeverity = 'all';
  keyword = '';
  onlyRecommended = false;
  onlyAvailable = true;
  stats: TemplateStats = { total: 0, available: 0, recommended: 0, categories: {} };

  directory = '';
  drawerVisible = false;
  drawerTitle = '';
  previewContent = '';
  previewFilename = 'alert-rule.yaml';
  previewLoading = false;
  activeTemplate?: TemplateViewModel;
  applyNamespace = 'polardbx-monitor';
  applyName = '';
  dryRunWarnings: string[] = [];

  private cache = new Map<string, AlertRuleTemplateDetail>();
  private categoryMap = new Map<string, AlertRuleTemplateCategory>();

  ngOnInit(): void {
    this.loadData();
  }

  reload(): void {
    this.loadData(true);
  }

  applyFilters(): void {
    const keyword = this.keyword.trim().toLowerCase();
    this.filteredTemplates = this.templates.filter(template => {
      if (this.onlyAvailable && !template.available) {
        return false;
      }
      if (this.onlyRecommended && !template.recommended) {
        return false;
      }
      if (this.selectedCategory !== 'all' && template.categoryId !== this.selectedCategory) {
        return false;
      }
      if (this.selectedSeverity !== 'all' && template.severity !== this.selectedSeverity) {
        return false;
      }
      if (!keyword) {
        return true;
      }
      const haystack = [
        template.name,
        template.title,
        template.description,
        template.category ?? '',
        template.severity ?? '',
        ...template.tags
      ].join('|').toLowerCase();
      return haystack.includes(keyword);
    });
  }

  async openPreview(template: TemplateViewModel): Promise<void> {
    if (!template.available) {
      this.message.warning('该模板尚未同步到后端目录');
      return;
    }

    this.drawerVisible = true;
    this.drawerTitle = `${template.title} (${template.name})`;
    this.previewFilename = template.file || `${template.name}.yaml`;
    this.activeTemplate = template;
    this.previewLoading = true;
    this.dryRunWarnings = [];

    try {
      const detail = await this.fetchTemplateDetail(template.name);
  this.previewContent = detail.content;
  this.applyNamespace = template.defaultNamespace || detail.labels?.['namespace'] || 'polardbx-monitor';
      this.applyName = detail.name || template.name;
      setTimeout(() => {
        if (this.yamlPreview) {
          this.yamlPreview.clearError();
        }
      });
    } catch (error) {
      console.error('加载模板内容失败:', error);
      this.previewContent = '';
      this.message.error('加载模板内容失败');
    } finally {
      this.previewLoading = false;
    }
  }

  closeDrawer(): void {
    this.drawerVisible = false;
    this.previewContent = '';
    this.activeTemplate = undefined;
    this.dryRunWarnings = [];
  }

  async quickApply(template: TemplateViewModel): Promise<void> {
    if (!template.available) {
      this.message.warning('该模板尚未同步到后端目录');
      return;
    }

    try {
      const detail = await this.fetchTemplateDetail(template.name);
      const payload: ApplyAlertRuleTemplatePayload = {
        template: template.name,
        namespace: template.defaultNamespace || 'polardbx-monitor',
        name: detail.name || template.name,
        overwrite: false
      };

      const content = `命名空间：${payload.namespace}\n资源名称：${payload.name}\n应用此模板将创建 PrometheusRule 对象。`;
      this.modal.confirm({
        nzTitle: '确认应用告警模板？',
        nzContent: content,
        nzOkText: '应用',
        nzOkType: 'primary',
        nzCancelText: '取消',
        nzOnOk: () => this.executeQuickApply(payload)
      });
    } catch (error: unknown) {
      this.handleApplyError(error);
    }
  }

  private async executeQuickApply(payload: ApplyAlertRuleTemplatePayload): Promise<void> {
    try {
      this.applying = true;
      await lastValueFrom(this.api.applyAlertRuleTemplate(payload));
      this.message.success('PrometheusRule 已创建');
    } catch (error: unknown) {
      this.handleApplyError(error);
      throw error;
    } finally {
      this.applying = false;
    }
  }

  async applyFromDrawer(): Promise<void> {
    if (!this.activeTemplate) {
      return;
    }

    const yamlContent = this.yamlPreview?.yamlContent || this.previewContent;
    if (!yamlContent.trim()) {
      this.message.warning('没有可应用的 YAML 内容');
      return;
    }

    const payload: ApplyAlertRuleTemplatePayload = {
      content: yamlContent,
      namespace: this.applyNamespace.trim() || 'polardbx-monitor',
      name: this.applyName.trim() || this.activeTemplate.name,
      overwrite: true
    };

    try {
      this.applying = true;
      const resp = await lastValueFrom(this.api.applyAlertRuleTemplate(payload));
      this.message.success(`PrometheusRule ${resp.name} 已更新`);
      this.closeDrawer();
    } catch (error: unknown) {
      this.handleApplyError(error);
    } finally {
      this.applying = false;
    }
  }

  async dryRun(): Promise<void> {
    if (!this.activeTemplate) {
      return;
    }

    const yamlContent = this.yamlPreview?.yamlContent || this.previewContent;
    if (!yamlContent.trim()) {
      this.message.warning('没有可校验的内容');
      return;
    }

    const payload: ApplyAlertRuleTemplatePayload = {
      content: yamlContent,
      namespace: this.applyNamespace.trim() || 'polardbx-monitor',
      name: this.applyName.trim() || this.activeTemplate.name,
      dryRun: true
    };

    try {
      this.loadingService.setLoading(LoadingKeys.ALERT_TEMPLATE_DETAIL, true);
      const resp = await lastValueFrom(this.api.applyAlertRuleTemplate(payload));
      const validation = resp.validation;
      if (validation) {
        this.dryRunWarnings = validation.warnings || [];
        if (validation.success) {
          this.message.success(validation.message || 'Dry-run 校验通过');
        } else {
          this.message.error(validation.message || 'Dry-run 校验失败');
        }
      } else {
        this.message.info('后端未返回 Dry-run 校验结果');
      }
    } catch (error: unknown) {
      this.handleApplyError(error);
    } finally {
      this.loadingService.setLoading(LoadingKeys.ALERT_TEMPLATE_DETAIL, false);
    }
  }

  validateYaml = async (yaml: string): Promise<{ success: boolean; message: string }> => {
    const resp = await lastValueFrom(this.api.validatePrometheusRule(yaml));
    return {
      success: Boolean(resp?.success),
      message: (resp?.message as string) || (resp?.error as string) || '校验完成'
    };
  };

  private loadData(force = false): void {
    if (this.loading) {
      return;
    }

    if (!force && this.initialized && this.templates.length) {
      this.applyFilters();
      return;
    }

    this.loading = true;
    this.loadingService.setLoading(LoadingKeys.ALERT_TEMPLATE_LIST, true);

    const manifest$ = this.http
      .get<AlertRuleTemplateManifest>('assets/monitoring/alert-rule-templates.json')
      .pipe(catchError(error => {
        console.warn('加载告警模板清单失败，使用空清单:', error);
        return of({ templates: [] } as AlertRuleTemplateManifest);
      }));

    const summaries$ = this.api
      .listAlertRuleTemplates()
      .pipe(catchError(error => {
        console.error('加载告警模板失败:', error);
        this.message.error('无法读取告警模板目录，请确认后端部署包含 charts 目录。');
        return of({ items: [] as AlertRuleTemplateSummary[], directory: '' });
      }));

    forkJoin({ manifest: manifest$, summaries: summaries$ })
      .pipe(finalize(() => {
        this.loading = false;
        this.loadingService.setLoading(LoadingKeys.ALERT_TEMPLATE_LIST, false);
        this.initialized = true;
      }))
      .subscribe(({ manifest, summaries }) => {
        this.buildCategoryMap(manifest.categories ?? []);
        this.directory = summaries.directory || '';
        this.templates = this.mergeTemplates(manifest.templates ?? [], summaries.items ?? []);
        this.categories = this.buildCategoryOptions();
        this.severities = Array.from(
          new Set(this.templates.map(t => t.severity).filter((sev): sev is string => !!sev))
        ).sort();
        this.updateStats();
        this.applyFilters();
      });
  }

  private mergeTemplates(manifestList: AlertRuleTemplateMeta[], summaries: AlertRuleTemplateSummary[]): TemplateViewModel[] {
    const manifestMap = new Map<string, AlertRuleTemplateMeta>();
    manifestList.forEach(meta => manifestMap.set(meta.name, meta));

    const merged: TemplateViewModel[] = [];

    summaries.forEach(summary => {
      const meta = manifestMap.get(summary.name);
      if (meta) {
        manifestMap.delete(summary.name);
      }
      const categoryId = meta?.category || summary.categories?.[0];
      const resolvedCategory = this.resolveCategory(categoryId, meta?.category || summary.categories?.[0]);
      merged.push({
        name: summary.name,
        title: meta?.title || summary.title || summary.name,
        description: this.getTemplateDescription(meta, summary),
        categoryId: resolvedCategory.id,
        category: resolvedCategory.label,
        severity: meta?.severity || summary.primarySeverity,
        tags: this.getTemplateTags(meta, summary),
        recommended: Boolean(meta?.recommended),
        available: true,
        source: summary.source,
        file: summary.file,
        groups: this.formatGroupList(summary.groups),
        summary,
        manifest: meta,
        docLink: meta?.docLink,
        defaultNamespace: meta?.defaultNamespace || summary.labels?.['namespace']
      });
    });

    manifestMap.forEach(meta => {
      const resolvedCategory = this.resolveCategory(meta.category, meta.category);
      merged.push({
        name: meta.name,
        title: meta.title,
        description: this.getTemplateDescription(meta),
        categoryId: resolvedCategory.id,
        category: resolvedCategory.label,
        severity: meta.severity,
  tags: this.getTemplateTags(meta),
        recommended: Boolean(meta.recommended),
        available: false,
        groups: [],
        manifest: meta,
        docLink: meta.docLink,
        defaultNamespace: meta.defaultNamespace
      });
    });

    return merged.sort((a, b) => a.title.localeCompare(b.title));
  }

  private buildCategoryMap(categories: AlertRuleTemplateCategory[]): void {
    this.categoryMap.clear();
    categories.forEach(category => this.categoryMap.set(category.id, category));
  }

  private buildCategoryOptions(): { id: string; label: string }[] {
    const optionMap = new Map<string, string>();
    this.categoryMap.forEach(category => optionMap.set(category.id, category.name));
    this.templates.forEach(template => {
      if (!template.categoryId) {
        return;
      }
      const label = template.category
        ?? this.categoryMap.get(template.categoryId)?.name
        ?? template.categoryId;
      optionMap.set(template.categoryId, label);
    });
    return Array.from(optionMap.entries())
      .map(([id, label]) => ({ id, label }))
      .sort((a, b) => a.label.localeCompare(b.label, 'zh'));
  }

  private resolveCategory(categoryId?: string, fallbackLabel?: string): { id?: string; label?: string } {
    const normalizedId = categoryId?.trim();
    if (normalizedId) {
      const found = this.categoryMap.get(normalizedId);
      if (found) {
        return { id: found.id, label: found.name };
      }
      const fallback = fallbackLabel && fallbackLabel.trim() ? fallbackLabel.trim() : normalizedId;
      return { id: normalizedId, label: fallback };
    }
    if (fallbackLabel?.trim()) {
      return { label: fallbackLabel.trim() };
    }
    return {};
  }

  private getTemplateDescription(meta?: AlertRuleTemplateMeta, summary?: AlertRuleTemplateSummary): string {
    const manifestDescription = meta?.description?.trim();
    if (manifestDescription) {
      return manifestDescription;
    }

    const summaryDescription = summary?.description?.trim();
    if (summaryDescription && !this.looksLikePromql(summaryDescription)) {
      return summaryDescription;
    }

    const groupSummary = this.composeGroupDescription(summary?.groups);
    if (groupSummary) {
      return groupSummary;
    }

    return '查看模板详情了解规则内容。';
  }

  private looksLikePromql(text?: string): boolean {
    if (!text) {
      return false;
    }
    const sample = text.trim();
    if (!sample) {
      return false;
    }
    if (sample.includes('\n')) {
      return true;
    }
    if (/[{}]/.test(sample)) {
      return true;
    }
    if (/(rate|sum|avg|count|histogram_quantile|max|min)\s*\(/i.test(sample)) {
      return true;
    }
    if (/\bby\s*\(|\bover_time\b/i.test(sample)) {
      return true;
    }
    return sample.length > 160;
  }

  private composeGroupDescription(groups?: AlertRuleTemplateSummary['groups']): string | undefined {
    if (!groups?.length) {
      return undefined;
    }
    const totalRules = groups.reduce((acc, group) => acc + (group.rules ?? 0), 0);
    if (!totalRules) {
      return undefined;
    }
    const groupNames = groups.map(group => group.name).filter(Boolean);
    if (!groupNames.length) {
      return `共 ${totalRules} 条规则，查看详情了解具体内容。`;
    }
    const displayNames = groupNames.slice(0, 3).join('、');
    const suffix = groupNames.length > 3 ? ' 等' : '';
    return `包含 ${totalRules} 条规则，覆盖 ${displayNames}${suffix}。`;
  }

  private getTemplateTags(meta?: AlertRuleTemplateMeta, summary?: AlertRuleTemplateSummary): string[] {
    const tags = meta?.tags?.length ? meta.tags : summary?.groups?.flatMap(group => group.severities ?? []) ?? [];
    return Array.from(new Set(tags.filter(tag => !!tag)));
  }

  private formatGroupList(groups?: AlertRuleTemplateSummary['groups']): string[] {
    return (groups ?? []).map(group => `${group.name} (${group.rules})`);
  }

  private async fetchTemplateDetail(name: string): Promise<AlertRuleTemplateDetail> {
    if (this.cache.has(name)) {
      return this.cache.get(name)!;
    }
    this.loadingService.setLoading(LoadingKeys.ALERT_TEMPLATE_DETAIL, true);
    try {
      const detail = await lastValueFrom(this.api.getAlertRuleTemplate(name));
      this.cache.set(name, detail);
      return detail;
    } finally {
      this.loadingService.setLoading(LoadingKeys.ALERT_TEMPLATE_DETAIL, false);
    }
  }

  private updateStats(): void {
    const total = this.templates.length;
    const available = this.templates.filter(t => t.available).length;
    const recommended = this.templates.filter(t => t.recommended).length;
    const categories = this.templates.reduce<Record<string, number>>((acc, cur) => {
      const key = cur.category ?? '未分组';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    this.stats = { total, available, recommended, categories };
  }

  private handleApplyError(error: unknown): void {
    console.error('应用告警模板失败:', error);
    const errObj = error as { error?: { error?: string; details?: string }; message?: string; status?: number };
    const detail = errObj?.error?.details || errObj?.error?.error || errObj?.message || '未知错误';
    this.message.error(`应用失败：${detail}`);
  }
}
