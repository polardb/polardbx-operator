import { Injectable, inject } from '@angular/core';
import { BehaviorSubject, Observable, timer, from, of, lastValueFrom } from 'rxjs';
import { switchMap, catchError, map } from 'rxjs/operators';
import { ApiService } from './api.service';

export interface InstallTask {
  kind: 'monitoring' | 'logs';
  jobName: string;
  namespace: string;
  targetNamespace?: string;
  phase?: string;
  message?: string;
  createdAt?: number;
  lastProbeAt?: number;
}

interface BootstrapStatusDto {
  phase?: string;
  failureReason?: string;
  message?: string;
}

interface ErrorLike {
  status?: number;
  code?: number;
  error?: { code?: number };
}

@Injectable({ providedIn: 'root' })
export class GlobalInstallProgressService {
  private readonly STORAGE_KEY = 'polardbx.global.installTasks';
  private tasks$ = new BehaviorSubject<InstallTask[]>(this.loadFromStorage());
  private readonly TTL_MS = 2 * 60 * 60 * 1000; // 2h
  private readonly api = inject(ApiService);

  constructor() {
    // 周期性轮询更新任务状态（仅当列表非空）
    timer(0, 5000)
      .pipe(
        switchMap(() => this.refreshAllTasks())
      )
      .subscribe();
  }

  getTasks(): Observable<InstallTask[]> {
    return this.tasks$.asObservable();
  }

  reportMonitoringInstall(jobName: string, namespace: string, targetNamespace?: string): void {
    this.upsert({ kind: 'monitoring', jobName, namespace, targetNamespace });
  }

  reportLogsInstall(jobName: string, namespace: string, targetNamespace?: string): void {
    this.upsert({ kind: 'logs', jobName, namespace, targetNamespace });
  }

  dismiss(task: InstallTask): void {
    const next = this.tasks$.value.filter(t => !(t.kind === task.kind && t.jobName === task.jobName && t.namespace === task.namespace));
    const snapshot = next.map(it => ({ ...it }));
    this.tasks$.next(snapshot);
    this.saveToStorage(snapshot);
  }

  private upsert(task: InstallTask): void {
    const now = Date.now();
    const list = [...this.tasks$.value];
    const idx = list.findIndex(t => t.kind === task.kind && t.jobName === task.jobName && t.namespace === task.namespace);
    const nextTask = { ...list[idx], ...task, createdAt: list[idx]?.createdAt || now, lastProbeAt: now };
    if (idx >= 0) {
      list[idx] = nextTask;
    } else {
      list.unshift(nextTask);
    }
    const next = list.map(it => ({ ...it }));
    this.tasks$.next(next);
    this.saveToStorage(next);
  }

  private refreshAllTasks(): Observable<void> {
    const current = this.tasks$.value;
    if (!current.length) return of(void 0);

    // TTL 清理过期任务
    const now = Date.now();
    const fresh = current
      .filter((t) => {
        const ts = t.lastProbeAt || t.createdAt;
        return !ts || (now - ts) < this.TTL_MS;
      })
      .map((t) => ({ ...t }));
    if (fresh.length !== current.length) {
      const next = fresh.map(it => ({ ...it }));
      this.tasks$.next(next);
      this.saveToStorage(next);
    }

    if (!fresh.length) {
      return of(void 0);
    }

    const toRemove: InstallTask[] = [];
    const updates: Promise<void>[] = fresh.map(async (t) => {
      try {
        if (t.kind === 'monitoring') {
          const status = await lastValueFrom(
            this.api.monitoringBootstrapStatus(t.jobName, t.namespace) as Observable<BootstrapStatusDto>
          );
          t.phase = status?.phase;
          t.message = status?.failureReason || status?.message || '';
        } else {
          const status = await lastValueFrom(
            this.api.logsBootstrapStatus(t.jobName, t.namespace) as Observable<BootstrapStatusDto>
          );
          t.phase = status?.phase;
          t.message = status?.failureReason || status?.message || '';
        }
        t.lastProbeAt = now;
        if (!t.createdAt) {
          t.createdAt = now;
        }
      } catch (err: unknown) {
        // 若任务不存在（404），从队列中移除，避免持续轮询报错
        const code = this.extractErrorCode(err);
        if (code === 404) {
          toRemove.push(t);
        }
        // 其他错误暂时忽略
      }
    });
    return from(Promise.all(updates)).pipe(
      map(() => {
        let updatedList = this.tasks$.value;
        // 合并刷新结果
        if (fresh.length) {
          const merged = updatedList.map((item) => {
            const match = fresh.find(f => f.kind === item.kind && f.jobName === item.jobName && f.namespace === item.namespace);
            return match ? { ...match } : item;
          });
          updatedList = merged;
        }

        if (toRemove.length) {
          updatedList = updatedList.filter(
            (it) => !toRemove.some(r => r.kind === it.kind && r.jobName === it.jobName && r.namespace === it.namespace)
          );
          toRemove
            .filter(t => t.kind === 'monitoring')
            .forEach(t => this.clearMonitoringWizardState(t.jobName, t.namespace));
        }

        const next = updatedList.map(it => ({ ...it }));
        this.tasks$.next(next);
        this.saveToStorage(next);
        return void 0;
      }),
      catchError(() => of(void 0))
    );
  }

  private loadFromStorage(): InstallTask[] {
    try {
      const raw = localStorage.getItem(this.STORAGE_KEY);
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map((item: unknown) => {
          const record = (item || {}) as Record<string, unknown>;
          const jobName = typeof record['jobName'] === 'string' ? record['jobName'] as string : '';
          const namespace = typeof record['namespace'] === 'string' ? record['namespace'] as string : '';
          const targetNamespace = typeof record['targetNamespace'] === 'string' ? record['targetNamespace'] as string : undefined;
          const kind: 'monitoring' | 'logs' = record['kind'] === 'logs' ? 'logs' : 'monitoring';
          const task: InstallTask = {
            kind,
            jobName,
            namespace,
            targetNamespace,
            phase: typeof record['phase'] === 'string' ? record['phase'] as string : undefined,
            message: typeof record['message'] === 'string' ? record['message'] as string : undefined,
            createdAt: typeof record['createdAt'] === 'number' ? record['createdAt'] as number : Date.now(),
            lastProbeAt: typeof record['lastProbeAt'] === 'number' ? record['lastProbeAt'] as number : undefined
          };
          return task;
        })
        .filter((t) => !!t.jobName && !!t.namespace);
    } catch {
      return [];
    }
  }

  private saveToStorage(list: InstallTask[]): void {
    try {
      localStorage.setItem(this.STORAGE_KEY, JSON.stringify(list));
    } catch {
      // ignore
    }
  }

  private extractErrorCode(err: unknown): number | undefined {
    if (!err || typeof err !== 'object') {
      return undefined;
    }
    const candidate = err as ErrorLike;
    return candidate.status ?? candidate.code ?? candidate.error?.code;
  }

  private clearMonitoringWizardState(jobName: string, namespace: string): void {
    try {
      const raw = localStorage.getItem('polardbx-monitor-install-job');
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (parsed?.jobName === jobName && parsed?.namespace === namespace) {
        localStorage.removeItem('polardbx-monitor-install-job');
      }
    } catch {
      // ignore
    }
  }
}