import { TestBed } from '@angular/core/testing';
import { firstValueFrom, take } from 'rxjs';
import {
  MONITORING_CHECKPOINT_STORE,
  MonitoringInstallationStateService,
  type Checkpoint,
  type CheckpointStore,
  type InstallStatusResponse,
  type PersistedCheckpoint
} from './monitoring-installation-state.service';

const cloneValue = <T>(value: T): T => JSON.parse(JSON.stringify(value));

class InMemoryCheckpointStore implements CheckpointStore {
  private readonly storage = new Map<string, PersistedCheckpoint>();

  async save(key: string, value: PersistedCheckpoint): Promise<void> {
    this.storage.set(key, this.cloneRecord(value));
  }

  async load(key: string): Promise<PersistedCheckpoint | null> {
    const record = this.storage.get(key);
    return record ? this.cloneRecord(record) : null;
  }

  async remove(key: string): Promise<void> {
    this.storage.delete(key);
  }

  async purgeExpired(now: number): Promise<void> {
    for (const [key, record] of Array.from(this.storage.entries())) {
      if (record.expiresAt <= now) {
        this.storage.delete(key);
      }
    }
  }

  snapshot(key: string): PersistedCheckpoint | undefined {
    return this.storage.get(key);
  }

  private cloneRecord(record: PersistedCheckpoint): PersistedCheckpoint {
    return {
      key: record.key,
      checkpoint: cloneValue(record.checkpoint),
      sessionId: record.sessionId,
      sessions: record.sessions ? record.sessions.map(item => ({
        sessionId: item.sessionId,
        checkpoint: cloneValue(item.checkpoint),
        updatedAt: item.updatedAt
      })) : undefined,
      updatedAt: record.updatedAt,
      expiresAt: record.expiresAt
    };
  }
}

describe('MonitoringInstallationStateService', () => {
  let service: MonitoringInstallationStateService;
  let store: InMemoryCheckpointStore;

  beforeEach(() => {
    store = new InMemoryCheckpointStore();
    TestBed.configureTestingModule({
      providers: [
        MonitoringInstallationStateService,
        { provide: MONITORING_CHECKPOINT_STORE, useValue: store }
      ]
    });
    service = TestBed.inject(MonitoringInstallationStateService);
  });

  it('persists checkpoints and updates session history', async () => {
    await service.hydrate({ namespace: 'polardbx-monitor' });

    const checkpoint: Checkpoint = {
      sessionId: 'session-a',
      phase: 'Installing',
      progress: 0.35,
      components: [
        {
          name: 'prometheus',
          phase: 'Installing'
        }
      ],
      context: {
        namespace: 'polardbx-monitor'
      }
    };

    await service.persistCheckpoint(checkpoint);

    const snapshot = service.getSnapshot();
    expect(snapshot.sessionId).toBe('session-a');
    expect(snapshot.components['prometheus'].phase).toBe('Installing');

    const stored = store.snapshot('polardbx-monitor');
    expect(stored).toBeTruthy();
    expect(stored?.sessions?.length).toBe(1);
    expect(stored?.sessions?.[0].sessionId).toBe('session-a');

    const history = service.getCheckpointHistory();
    expect(history.length).toBe(1);
    expect(history[0].sessionId).toBe('session-a');
  });

  it('hydrates from persisted history and prefers most recent session', async () => {
    const now = Date.now();
    const record: PersistedCheckpoint = {
      key: 'polardbx-monitor',
      checkpoint: {
        sessionId: 'session-new',
        phase: 'Verifying',
        context: {
          namespace: 'polardbx-monitor'
        }
      },
      sessionId: 'session-new',
      sessions: [
        {
          sessionId: 'session-new',
          checkpoint: {
            sessionId: 'session-new',
            phase: 'Verifying',
            context: {
              namespace: 'polardbx-monitor'
            }
          },
          updatedAt: now
        },
        {
          sessionId: 'session-old',
          checkpoint: {
            sessionId: 'session-old',
            phase: 'Installing',
            context: {
              namespace: 'polardbx-monitor'
            }
          },
          updatedAt: now - 10_000
        }
      ],
      updatedAt: now,
      expiresAt: now + 86_400_000
    };
    await store.save(record.key, record);

    await service.hydrate({ namespace: 'polardbx-monitor' });

    const snapshot = service.getSnapshot();
    expect(snapshot.sessionId).toBe('session-new');
    expect(snapshot.phase).toBe('Verifying');

    const history = service.getCheckpointHistory();
    expect(history.length).toBe(2);
    expect(history[0].sessionId).toBe('session-new');
    expect(history[1].sessionId).toBe('session-old');
  });

  it('clears persisted checkpoint and history', async () => {
    await service.hydrate({ namespace: 'polardbx-monitor' });

    const checkpoint: Checkpoint = {
      sessionId: 'session-clean',
      phase: 'Installing',
      context: {
        namespace: 'polardbx-monitor'
      }
    };

    await service.persistCheckpoint(checkpoint);
    await service.clearCheckpoint();

    const stored = store.snapshot('polardbx-monitor');
    expect(stored).toBeUndefined();

    const snapshot = service.getSnapshot();
    expect(snapshot.sessionId).toBeUndefined();
    expect(snapshot.checkpoint).toBeNull();
    expect(service.getCheckpointHistory().length).toBe(0);
  });

  it('updates state from status responses and emits via observable', async () => {
    await service.hydrate({ namespace: 'polardbx-monitor', clusterId: 'cluster-1' });

    const initial = await firstValueFrom(service.getState().pipe(take(1)));
    expect(initial.phase).toBe('NotStarted');

    const response: InstallStatusResponse = {
      sessionId: 'session-status',
      phase: 'Installing',
      progress: 0.42,
      components: [
        { name: 'prometheus', phase: 'Installing', retryCount: 1 },
        { name: 'grafana', phase: 'Planned' }
      ],
      currentStep: { order: 2, component: 'grafana', action: 'install', status: 'running' },
      completedSteps: [
        { order: 1, component: 'prometheus', action: 'install', status: 'succeeded' }
      ],
      errors: [
        { category: 'Timeout', message: 'delayed rollout' }
      ],
      startedAt: '2025-10-28T01:00:00Z',
      updatedAt: '2025-10-28T01:05:00Z',
      checkpoint: {
        sessionId: 'session-status',
        phase: 'Installing',
        progress: 0.42,
        context: { namespace: 'polardbx-monitor', clusterId: 'cluster-1' }
      }
    };

    service.updateFromStatus(response);

    const snapshot = service.getSnapshot();
    expect(snapshot.sessionId).toBe('session-status');
    expect(snapshot.phase).toBe('Installing');
    expect(snapshot.progress).toBeCloseTo(0.42, 5);
    expect(Object.keys(snapshot.components)).toEqual(['prometheus', 'grafana']);
    expect(snapshot.currentStep?.component).toBe('grafana');
    expect(snapshot.completedSteps.length).toBe(1);
    expect(snapshot.errors[0].category).toBe('Timeout');
    expect(snapshot.startedAt).toBe('2025-10-28T01:00:00Z');
    expect(snapshot.updatedAt).toBe('2025-10-28T01:05:00Z');
    expect(snapshot.checkpoint?.sessionId).toBe('session-status');

    const emitted = await firstValueFrom(service.getState().pipe(take(1)));
    expect(emitted.sessionId).toBe('session-status');
  });

  it('clamps progress and merges component updates', async () => {
    await service.hydrate({ namespace: 'polardbx-monitor' });

    service.updateProgress(1.5);
    expect(service.getSnapshot().progress).toBe(1);

    service.updateProgress(-0.2);
    expect(service.getSnapshot().progress).toBe(0);

  service.upsertComponent({ name: 'prometheus', phase: 'Installing' });
  service.upsertComponent({ name: 'grafana', phase: 'Healthy' });

    const { components } = service.getSnapshot();
    expect(Object.keys(components)).toEqual(['prometheus', 'grafana']);
  expect(components['grafana'].phase).toBe('Healthy');
  });

  it('appends and clears errors in sequence', async () => {
    await service.hydrate({ namespace: 'polardbx-monitor' });

    service.appendError({ category: 'Timeout', message: 'first' });
    service.appendError({ category: 'HelmChart', message: 'second' });

    expect(service.getSnapshot().errors.map(error => error.message)).toEqual(['first', 'second']);

    service.clearErrors();
  expect(service.getSnapshot().errors.length).toBe(0);
  });

  it('maintains bounded session history when persisting multiple checkpoints', async () => {
    await service.hydrate({ namespace: 'polardbx-monitor' });

    for (let i = 0; i < 7; i += 1) {
      await service.persistCheckpoint({
        sessionId: `session-${i}`,
        phase: 'Installing',
        context: { namespace: 'polardbx-monitor' }
      });
    }

    const history = service.getCheckpointHistory();
    expect(history.length).toBeLessThanOrEqual(5);
    expect(history[0].sessionId).toBe('session-6');
    expect(history.at(-1)?.sessionId).toBe('session-2');
  });
});
