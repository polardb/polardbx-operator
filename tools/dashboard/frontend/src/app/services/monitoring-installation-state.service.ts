import { Injectable, InjectionToken, inject } from '@angular/core';
import { BehaviorSubject, Observable } from 'rxjs';
import type { components } from '../models/generated/monitoring-installation';

export type InstallPhase = components['schemas']['InstallPhase'];
export type ComponentState = components['schemas']['ComponentState'];
export type InstallError = components['schemas']['InstallError'];
export type Checkpoint = components['schemas']['Checkpoint'];
export type InstallStatusResponse = components['schemas']['InstallStatusResponse'];

export interface CheckpointHistoryEntry {
  sessionId?: string;
  checkpoint: Checkpoint;
  updatedAt: number;
}

export interface InstallationScope {
  clusterId?: string;
  namespace: string;
}

export interface InstallationState {
  sessionId?: string;
  scope?: InstallationScope;
  phase: InstallPhase;
  progress: number;
  components: Record<string, ComponentState>;
  currentStep?: components['schemas']['ExecutionStep'];
  completedSteps: components['schemas']['ExecutionStep'][];
  errors: InstallError[];
  startedAt?: string;
  updatedAt?: string;
  checkpoint?: Checkpoint | null;
}

const STORAGE_NAME = 'polardbx-monitoring-install';
const STORE_NAME = 'checkpoints';
const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;
const MAX_HISTORY = 5;

export interface PersistedCheckpoint {
  key: string;
  checkpoint: Checkpoint;
  sessionId?: string | null;
  sessions?: CheckpointHistoryEntry[];
  updatedAt: number;
  expiresAt: number;
}

export interface CheckpointStore {
  save(key: string, value: PersistedCheckpoint): Promise<void>;
  load(key: string): Promise<PersistedCheckpoint | null>;
  remove(key: string): Promise<void>;
  purgeExpired(now: number): Promise<void>;
}

class IndexedDbCheckpointStore implements CheckpointStore {
  private dbPromise: Promise<IDBDatabase> | null = null;

  private get supported(): boolean {
    return typeof window !== 'undefined' && typeof window.indexedDB !== 'undefined';
  }

  private async openDb(): Promise<IDBDatabase> {
    if (!this.supported) {
      throw new Error('IndexedDB not supported');
    }
    if (!this.dbPromise) {
      this.dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
        const request = window.indexedDB.open(STORAGE_NAME, 1);
        request.onupgradeneeded = () => {
          const db = request.result;
          if (!db.objectStoreNames.contains(STORE_NAME)) {
            db.createObjectStore(STORE_NAME, { keyPath: 'key' });
          }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error ?? new Error('IndexedDB open failed'));
      });
    }
    try {
      return await this.dbPromise;
    } catch (error) {
      this.dbPromise = null;
      throw error;
    }
  }

  private async withStore(mode: IDBTransactionMode): Promise<IDBObjectStore> {
    const db = await this.openDb();
    const tx = db.transaction(STORE_NAME, mode);
    return tx.objectStore(STORE_NAME);
  }

  async save(key: string, value: PersistedCheckpoint): Promise<void> {
    const store = await this.withStore('readwrite');
    await new Promise<void>((resolve, reject) => {
      const request = store.put(value);
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error ?? new Error('IndexedDB save failed'));
    });
  }

  async load(key: string): Promise<PersistedCheckpoint | null> {
    try {
      const store = await this.withStore('readonly');
      return await new Promise<PersistedCheckpoint | null>((resolve, reject) => {
        const request = store.get(key);
        request.onsuccess = () => {
          resolve((request.result as PersistedCheckpoint | undefined) ?? null);
        };
        request.onerror = () => reject(request.error ?? new Error('IndexedDB read failed'));
      });
    } catch {
      return null;
    }
  }

  async remove(key: string): Promise<void> {
    try {
      const store = await this.withStore('readwrite');
      await new Promise<void>((resolve, reject) => {
        const request = store.delete(key);
        request.onsuccess = () => resolve();
        request.onerror = () => reject(request.error ?? new Error('IndexedDB delete failed'));
      });
    } catch {
      // swallow errors to allow fallback
    }
  }

  async purgeExpired(now: number): Promise<void> {
    try {
      const store = await this.withStore('readwrite');
      await new Promise<void>((resolve, reject) => {
        const request = store.openCursor();
        request.onsuccess = () => {
          const cursor = request.result;
          if (!cursor) {
            resolve();
            return;
          }
          const value = cursor.value as PersistedCheckpoint;
          if (value.expiresAt <= now) {
            cursor.delete();
          }
          cursor.continue();
        };
        request.onerror = () => reject(request.error ?? new Error('IndexedDB cursor failed'));
      });
    } catch {
      // ignore expired purge errors
    }
  }
}

class LocalStorageCheckpointStore implements CheckpointStore {
  private get available(): boolean {
    return typeof window !== 'undefined' && typeof window.localStorage !== 'undefined';
  }

  private keyName(key: string): string {
    return `${STORAGE_NAME}:${key}`;
  }

  async save(key: string, value: PersistedCheckpoint): Promise<void> {
    if (!this.available) return;
    window.localStorage.setItem(this.keyName(key), JSON.stringify(value));
  }

  async load(key: string): Promise<PersistedCheckpoint | null> {
    if (!this.available) return null;
    const raw = window.localStorage.getItem(this.keyName(key));
    if (!raw) return null;
    try {
      return JSON.parse(raw) as PersistedCheckpoint;
    } catch {
      window.localStorage.removeItem(this.keyName(key));
      return null;
    }
  }

  async remove(key: string): Promise<void> {
    if (!this.available) return;
    window.localStorage.removeItem(this.keyName(key));
  }

  async purgeExpired(now: number): Promise<void> {
    if (!this.available) return;
    const prefix = `${STORAGE_NAME}:`;
    for (let i = 0; i < window.localStorage.length; i += 1) {
      const k = window.localStorage.key(i);
      if (!k || !k.startsWith(prefix)) continue;
      const raw = window.localStorage.getItem(k);
      if (!raw) continue;
      try {
        const parsed = JSON.parse(raw) as PersistedCheckpoint;
        if (parsed.expiresAt <= now) {
          window.localStorage.removeItem(k);
        }
      } catch {
        window.localStorage.removeItem(k);
      }
    }
  }
}

function createStore(): CheckpointStore {
  try {
    return new IndexedDbCheckpointStore();
  } catch {
    return new LocalStorageCheckpointStore();
  }
}

export const MONITORING_CHECKPOINT_STORE = new InjectionToken<CheckpointStore>('MonitoringInstallationCheckpointStore');

@Injectable({ providedIn: 'root' })
export class MonitoringInstallationStateService {
  private readonly store: CheckpointStore = inject(MONITORING_CHECKPOINT_STORE, { optional: true }) ?? createStore();
  private readonly state$ = new BehaviorSubject<InstallationState>(this.createInitialState());
  private storageKey: string | null = null;
  private sessionHistory: CheckpointHistoryEntry[] = [];

  getState(): Observable<InstallationState> {
    return this.state$.asObservable();
  }

  getSnapshot(): InstallationState {
    return this.state$.value;
  }

  async hydrate(scope: InstallationScope): Promise<void> {
    this.storageKey = this.composeStorageKey(scope);
    this.state$.next({ ...this.createInitialState(), scope });
    this.sessionHistory = [];

    const now = Date.now();
    await this.store.purgeExpired(now);
    if (!this.storageKey) return;

    const persisted = await this.store.load(this.storageKey);
    if (!persisted) return;
    if (persisted.expiresAt <= now) {
      await this.store.remove(this.storageKey);
      return;
    }
    const normalized = this.normalizePersistedCheckpoint(persisted);
    this.sessionHistory = normalized.sessions ?? [];
    const selected = this.selectSessionEntry(normalized);
    const checkpoint = selected?.checkpoint ?? normalized.checkpoint;
    const sessionId = selected?.sessionId ?? normalized.sessionId ?? checkpoint.sessionId ?? undefined;
    this.state$.next({
      ...this.state$.value,
      scope,
      sessionId,
      checkpoint,
      phase: checkpoint.phase ?? this.state$.value.phase,
      progress: checkpoint.progress ?? this.state$.value.progress,
      components: this.listToComponentMap(checkpoint.components ?? []),
      completedSteps: this.state$.value.completedSteps,
      errors: checkpoint.errors ?? [],
      startedAt: checkpoint.startedAt ?? this.state$.value.startedAt,
      updatedAt: checkpoint.lastUpdatedAt ?? this.state$.value.updatedAt,
    });
  }

  updateFromStatus(status: InstallStatusResponse): void {
    const components = this.listToComponentMap(status.components ?? []);
    this.state$.next({
      ...this.state$.value,
      sessionId: status.sessionId,
      phase: status.phase ?? this.state$.value.phase,
      progress: status.progress ?? this.state$.value.progress,
      components,
      currentStep: status.currentStep ?? undefined,
      completedSteps: status.completedSteps ?? this.state$.value.completedSteps,
      errors: status.errors ?? [],
      startedAt: status.startedAt ?? this.state$.value.startedAt,
      updatedAt: status.updatedAt ?? new Date().toISOString(),
      checkpoint: status.checkpoint ?? this.state$.value.checkpoint,
    });
  }

  updatePhase(phase: InstallPhase): void {
    this.state$.next({ ...this.state$.value, phase });
  }

  updateProgress(progress: number): void {
    const clamped = Math.max(0, Math.min(1, progress));
    this.state$.next({ ...this.state$.value, progress: clamped });
  }

  upsertComponent(component: ComponentState): void {
    const components = { ...this.state$.value.components, [component.name ?? 'unknown']: component };
    this.state$.next({ ...this.state$.value, components });
  }

  appendError(error: InstallError): void {
    this.state$.next({ ...this.state$.value, errors: [...this.state$.value.errors, error] });
  }

  clearErrors(): void {
    this.state$.next({ ...this.state$.value, errors: [] });
  }

  async persistCheckpoint(checkpoint: Checkpoint, ttlMs: number = DEFAULT_TTL_MS): Promise<void> {
    const key = this.ensureStorageKeyFromCheckpoint(checkpoint);
    if (!key) {
      return;
    }

    const now = Date.now();
    const expiresAt = now + ttlMs;
    const existing = await this.store.load(key);
    const normalizedExisting = existing ? this.normalizePersistedCheckpoint(existing) : null;
    const sessionId = this.resolveSessionId(checkpoint, normalizedExisting);
    const normalizedCheckpoint = this.copyCheckpoint({ ...checkpoint, sessionId });
    const entry: CheckpointHistoryEntry = {
      sessionId,
      checkpoint: normalizedCheckpoint,
      updatedAt: now,
    };
    const sessions = this.upsertSessionHistory(normalizedExisting?.sessions ?? [], entry);
    const record: PersistedCheckpoint = {
      key,
      checkpoint: normalizedCheckpoint,
      sessionId,
      sessions,
      updatedAt: now,
      expiresAt,
    };

    try {
      await this.store.save(key, record);
    } catch {
      // fallback to localStorage if IndexedDB fails
      const fallback = new LocalStorageCheckpointStore();
      await fallback.save(key, record);
    }

    this.sessionHistory = sessions;
    this.state$.next({
      ...this.state$.value,
      sessionId,
      checkpoint: normalizedCheckpoint,
      phase: normalizedCheckpoint.phase ?? this.state$.value.phase,
      progress: normalizedCheckpoint.progress ?? this.state$.value.progress,
      components: this.listToComponentMap(normalizedCheckpoint.components ?? []),
      errors: normalizedCheckpoint.errors ?? this.state$.value.errors,
      startedAt: normalizedCheckpoint.startedAt ?? this.state$.value.startedAt,
      updatedAt: normalizedCheckpoint.lastUpdatedAt ?? this.state$.value.updatedAt,
    });
  }

  async clearCheckpoint(): Promise<void> {
    if (this.storageKey) {
      await this.store.remove(this.storageKey);
      const fallback = new LocalStorageCheckpointStore();
      await fallback.remove(this.storageKey);
    }
    this.sessionHistory = [];
    this.state$.next({ ...this.state$.value, sessionId: undefined, checkpoint: null });
  }

  reset(): void {
    const scope = this.state$.value.scope;
    this.storageKey = scope ? this.composeStorageKey(scope) : null;
    this.sessionHistory = [];
    this.state$.next({ ...this.createInitialState(), scope });
  }

  getCheckpointHistory(): CheckpointHistoryEntry[] {
    return this.sessionHistory.map(entry => ({
      sessionId: entry.sessionId,
      checkpoint: this.copyCheckpoint(entry.checkpoint),
      updatedAt: entry.updatedAt,
    }));
  }

  private createInitialState(): InstallationState {
    return {
      phase: 'NotStarted',
      progress: 0,
      components: {},
      completedSteps: [],
      errors: [],
      checkpoint: null,
    };
  }

  private composeStorageKey(scope: InstallationScope): string {
    const id = scope.clusterId ? `${scope.clusterId}-` : '';
    return `${id}${scope.namespace}`;
  }

  private listToComponentMap(list: ComponentState[]): Record<string, ComponentState> {
    return (list ?? []).reduce<Record<string, ComponentState>>((acc, item) => {
      if (item?.name) {
        acc[item.name] = item;
      }
      return acc;
    }, {});
  }

  private ensureStorageKeyFromCheckpoint(checkpoint: Checkpoint): string | null {
    if (this.storageKey) {
      return this.storageKey;
    }
    const scope = this.state$.value.scope;
    const contextNamespace = checkpoint.context?.['namespace'] ?? checkpoint.context?.['targetNamespace'];
    const configNamespace = checkpoint.config?.targetNamespace;
    const namespace = contextNamespace ?? configNamespace ?? scope?.namespace;
    if (!namespace) {
      return null;
    }
    const clusterId = checkpoint.context?.['clusterId'] ?? scope?.clusterId;
    const targetScope: InstallationScope = clusterId ? { namespace, clusterId } : { namespace };
    this.storageKey = this.composeStorageKey(targetScope);
    return this.storageKey;
  }

  private resolveSessionId(checkpoint: Checkpoint, existing?: PersistedCheckpoint | null): string | undefined {
  const candidates: (string | undefined | null)[] = [
      checkpoint.sessionId,
      this.state$.value.sessionId,
      existing?.sessionId ?? undefined,
      existing?.checkpoint.sessionId ?? undefined,
      existing?.sessions?.[0]?.sessionId ?? undefined,
      this.sessionHistory[0]?.sessionId ?? undefined,
    ];
    for (const candidate of candidates) {
      const trimmed = candidate?.trim();
      if (trimmed) {
        return trimmed;
      }
    }
    return undefined;
  }

  private normalizePersistedCheckpoint(record: PersistedCheckpoint): PersistedCheckpoint {
    const baseSessionId = record.sessionId ?? record.checkpoint.sessionId ?? undefined;
    const baseEntry: CheckpointHistoryEntry = {
      sessionId: baseSessionId,
      checkpoint: this.copyCheckpoint({ ...record.checkpoint, sessionId: baseSessionId ?? record.checkpoint.sessionId }),
      updatedAt: record.updatedAt,
    };
  const sessions = (record.sessions ?? [])
      .map(entry => ({
        sessionId: entry.sessionId ?? entry.checkpoint.sessionId ?? undefined,
        checkpoint: this.copyCheckpoint({ ...entry.checkpoint, sessionId: entry.sessionId ?? entry.checkpoint.sessionId ?? baseSessionId }),
        updatedAt: entry.updatedAt ?? record.updatedAt,
      }))
      .filter(entry => !!entry.checkpoint);
    const mergedSessions = this.upsertSessionHistory(sessions, baseEntry);
    return {
      ...record,
      sessionId: baseSessionId,
      checkpoint: baseEntry.checkpoint,
      sessions: mergedSessions,
    };
  }

  private selectSessionEntry(record: PersistedCheckpoint): CheckpointHistoryEntry | undefined {
    const sessions = record.sessions ?? [];
    if (!sessions.length) {
      return undefined;
    }
  const preferredIds: (string | undefined)[] = [
      this.state$.value.sessionId,
      record.sessionId ?? record.checkpoint.sessionId,
    ];
    for (const preferred of preferredIds) {
      if (!preferred) continue;
      const match = sessions.find(entry => this.normalizeSessionId(entry.sessionId) === this.normalizeSessionId(preferred));
      if (match) {
        return {
          sessionId: match.sessionId,
          checkpoint: this.copyCheckpoint(match.checkpoint),
          updatedAt: match.updatedAt,
        };
      }
    }
    const first = sessions[0];
    return {
      sessionId: first.sessionId,
      checkpoint: this.copyCheckpoint(first.checkpoint),
      updatedAt: first.updatedAt,
    };
  }

  private upsertSessionHistory(entries: CheckpointHistoryEntry[], entry: CheckpointHistoryEntry): CheckpointHistoryEntry[] {
    const normalizedKey = this.normalizeSessionId(entry.sessionId);
    const filtered = entries.filter(item => this.normalizeSessionId(item.sessionId) !== normalizedKey);
    const sanitized: CheckpointHistoryEntry = {
      sessionId: entry.sessionId,
      checkpoint: this.copyCheckpoint(entry.checkpoint),
      updatedAt: entry.updatedAt,
    };
    const combined = [sanitized, ...filtered]
      .sort((a, b) => b.updatedAt - a.updatedAt)
      .slice(0, MAX_HISTORY);
    return combined;
  }

  private copyCheckpoint(checkpoint: Checkpoint): Checkpoint {
    return {
      ...checkpoint,
      components: checkpoint.components ? checkpoint.components.map(component => ({ ...component })) : undefined,
      completedSteps: checkpoint.completedSteps ? [...checkpoint.completedSteps] : undefined,
      errors: checkpoint.errors
        ? checkpoint.errors.map(error => ({ ...error, context: error.context ? { ...error.context } : undefined }))
        : undefined,
      context: checkpoint.context ? { ...checkpoint.context } : undefined,
      config: checkpoint.config ? { ...checkpoint.config } : undefined,
    };
  }

  private normalizeSessionId(sessionId?: string | null): string {
    return sessionId?.trim() ? sessionId.trim() : '__anonymous__';
  }
}
