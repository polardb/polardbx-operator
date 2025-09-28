import { Injectable } from '@angular/core';
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
}

@Injectable({ providedIn: 'root' })
export class GlobalInstallProgressService {
  private readonly STORAGE_KEY = 'polardbx.global.installTasks';
  private tasks$ = new BehaviorSubject<InstallTask[]>(this.loadFromStorage());
  private readonly TTL_MS = 2 * 60 * 60 * 1000; // 2h

  constructor(private api: ApiService) {
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
    this.tasks$.next(next);
    this.saveToStorage(next);
  }

  private upsert(task: InstallTask): void {
    const list = this.tasks$.value.slice();
    const idx = list.findIndex(t => t.kind === task.kind && t.jobName === task.jobName && t.namespace === task.namespace);
    if (idx >= 0) list[idx] = { ...list[idx], ...task };
    else list.unshift(task);
    this.tasks$.next(list);
    this.saveToStorage(list);
  }

  private refreshAllTasks(): Observable<void> {
    const list = this.tasks$.value;
    if (!list.length) return of(void 0);

    // TTL 清理过期任务
    const now = Date.now();
    const fresh = list.filter((t: any) => !t._ts || (now - (t._ts as number)) < this.TTL_MS);
    if (fresh.length !== list.length) {
      this.tasks$.next(fresh);
      this.saveToStorage(fresh);
    }
    const toRemove: InstallTask[] = [];
    const updates: Promise<void>[] = list.map(async (t) => {
      try {
        if (t.kind === 'monitoring') {
          const status: any = await lastValueFrom(this.api.monitoringBootstrapStatus(t.jobName, t.namespace));
          t.phase = status?.phase;
          t.message = status?.failureReason || status?.message || '';
        } else {
          const status: any = await lastValueFrom(this.api.logsBootstrapStatus(t.jobName, t.namespace));
          t.phase = status?.phase;
          t.message = status?.failureReason || status?.message || '';
        }
        (t as any)._ts = now;
      } catch (err: any) {
        // 若任务不存在（404），从队列中移除，避免持续轮询报错
        const code = err?.status || err?.error?.code || err?.code;
        if (Number(code) === 404) {
          toRemove.push(t);
        }
        // 其他错误暂时忽略
      }
    });
    return from(Promise.all(updates)).pipe(
      map(() => {
        if (toRemove.length) {
          const next = this.tasks$.value.filter(
            (it) => !toRemove.some(r => r.kind === it.kind && r.jobName === it.jobName && r.namespace === it.namespace)
          );
          this.tasks$.next(next);
          this.saveToStorage(next);
        }
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
      return Array.isArray(parsed) ? parsed : [];
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
}