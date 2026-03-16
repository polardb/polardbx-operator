import { test, expect, Page } from '@playwright/test';

interface MonitoringScenario {
  detection: Record<string, unknown>;
  plan?: Record<string, unknown>;
  start?: Record<string, unknown>;
  statuses?: Record<string, Array<Record<string, unknown>>>;
  afterAutoFixStatuses?: Record<string, Array<Record<string, unknown>>>;
  diagnose?: Record<string, unknown>;
  autoFixResponses?: Record<string, Record<string, unknown>>;
}

async function setupMonitoringWizardMocks(page: Page, scenario: MonitoringScenario): Promise<void> {
  const statusQueues = new Map<string, Array<Record<string, unknown>>>();
  const lastStatus = new Map<string, Record<string, unknown>>();

  for (const [sessionId, responses] of Object.entries(scenario.statuses ?? {})) {
    statusQueues.set(sessionId, [...responses]);
  }

  await page.route('**/api/v1/**', async route => {
    const method = route.request().method();
    const url = new URL(route.request().url());
    const path = url.pathname.replace('/api/v1', '');

    if (method === 'OPTIONS') {
      await route.fulfill({ status: 200 });
      return;
    }

    if (path.startsWith('/monitoring/detect')) {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(scenario.detection),
      });
      return;
    }

    if (path === '/monitoring/plan' && method === 'POST') {
      const payload = scenario.plan ?? {
        plan: { riskLevel: 'low', steps: [] },
        estimatedDurationSeconds: 0,
      };
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      });
      return;
    }

    if (path === '/monitoring/install' && method === 'POST') {
      const body = scenario.start ?? {
        sessionId: 'e2e-session',
        namespace: 'polardbx-monitor',
        createdAt: new Date().toISOString(),
        resumed: false,
      };
      const bodyRecord = body as Record<string, unknown>;
      const sessionId = String(bodyRecord['sessionId'] ?? 'e2e-session');
      if (!statusQueues.has(sessionId) && scenario.statuses?.[sessionId]) {
        statusQueues.set(sessionId, [...scenario.statuses[sessionId]]);
      }
      await route.fulfill({
        status: 202,
        contentType: 'application/json',
        body: JSON.stringify(body),
      });
      return;
    }

    const statusMatch = path.match(/^\/monitoring\/install\/(.+)\/status$/);
    if (statusMatch) {
      const sessionId = decodeURIComponent(statusMatch[1]);
      if (!statusQueues.has(sessionId) && scenario.statuses?.[sessionId]) {
        statusQueues.set(sessionId, [...scenario.statuses[sessionId]]);
      }
      const queue = statusQueues.get(sessionId) ?? [];
      let payload: Record<string, unknown> | undefined;
      if (queue.length > 1) {
        payload = queue.shift();
      } else if (queue.length === 1) {
        payload = queue[0];
      }
      if (!payload) {
        payload = lastStatus.get(sessionId);
      }
      if (!payload) {
        payload = {
          sessionId,
          phase: 'Installing',
          progress: 0,
          updatedAt: new Date().toISOString(),
          components: [],
          completedSteps: [],
        };
      }
      lastStatus.set(sessionId, payload);
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      });
      return;
    }

    if (path === '/monitoring/diagnose' && method === 'POST') {
      const payload = scenario.diagnose ?? {
        diagnosis: {
          category: 'unknown',
          severity: 'minor',
          summary: 'No issues detected',
          possibleCauses: [],
          suggestedFixes: [],
        },
        autoFixes: [],
      };
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      });
      return;
    }

    if (path === '/monitoring/auto-fix' && method === 'POST') {
      const body = route.request().postDataJSON() as { sessionId?: string; fixId?: string } | null;
      const fixId = body?.fixId ?? 'unknown';
      const sessionId = body?.sessionId;
      if (sessionId && scenario.afterAutoFixStatuses?.[sessionId]) {
        statusQueues.set(sessionId, [...scenario.afterAutoFixStatuses[sessionId]]);
        lastStatus.delete(sessionId);
      }
      const payload = scenario.autoFixResponses?.[fixId] ?? {
        success: true,
        message: 'Auto-fix executed',
      };
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify(payload),
      });
      return;
    }

    if (path === '/monitoring/status' && method === 'GET') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ items: [] }),
      });
      return;
    }

    if (path === '/auth/me') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ enabled: false }),
      });
      return;
    }

    if (path === '/system/context') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ defaultNamespace: 'polardbx-monitor' }),
      });
      return;
    }

    if (path === '/system/namespaces') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          items: [
            {
              name: 'polardbx-monitor',
              status: 'Active',
              createdAt: '2025-10-20T10:00:00Z',
            },
          ],
          count: 1,
        }),
      });
      return;
    }

    if (method === 'GET') {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: '{}',
      });
      return;
    }

    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: '{}',
    });
  });
}

type InitScriptConfig = {
  script: (arg: any) => void;
  arg?: any;
};

function setAuthStorage(page: Page, extra?: (() => void) | InitScriptConfig): void {
  page.addInitScript(() => {
    window.localStorage.clear();
    window.sessionStorage.clear();
    window.localStorage.setItem('kubeconfig', 'e2e-kubeconfig');
    window.localStorage.setItem('activeNamespace', 'polardbx-monitor');
  });
  if (!extra) {
    return;
  }
  if (typeof extra === 'function') {
    page.addInitScript(extra);
  } else {
    page.addInitScript(extra.script, extra.arg);
  }
}

const detectionSnapshot = {
  namespace: 'polardbx-monitor',
  detectedAt: '2025-10-28T08:00:00Z',
  healthScore: 72,
  components: [
    {
      name: 'prometheus',
      exists: false,
      healthy: false,
      version: null,
      actionRecommendation: {
        action: 'install',
        reason: 'Component missing',
      },
    },
    {
      name: 'grafana',
      exists: true,
      healthy: true,
      version: 'v10.5.1',
      actionRecommendation: {
        action: 'skip',
        reason: 'Healthy',
      },
    },
  ],
  recommendations: ['Install the monitoring stack.'],
};

test('fresh install flow completes successfully', async ({ page }) => {
  setAuthStorage(page);

  const sessionId = 'sess-fresh-install';
  const scenario: MonitoringScenario = {
    detection: detectionSnapshot,
    plan: {
      plan: {
        sessionTemplate: {
          namespace: 'polardbx-monitor',
          releaseName: 'polardbx-monitor',
          intent: 'install',
          targetNamespace: 'polardbx-monitor',
        },
        riskLevel: 'medium',
        steps: [
          {
            order: 1,
            component: 'prometheus',
            action: 'install',
            estimatedDurationSeconds: 300,
            dependsOn: [],
            reason: 'Prometheus not detected',
          },
          {
            order: 2,
            component: 'grafana',
            action: 'upgrade',
            estimatedDurationSeconds: 180,
            dependsOn: [1],
            reason: 'Align with latest dashboard configuration',
          },
        ],
      },
      estimatedDurationSeconds: 480,
    },
    start: {
      sessionId,
      namespace: 'polardbx-monitor',
      createdAt: '2025-10-28T08:05:00Z',
      resumed: false,
    },
    statuses: {
      [sessionId]: [
        {
          sessionId,
          phase: 'Installing',
          progress: 0.45,
          updatedAt: '2025-10-28T08:06:30Z',
          startedAt: '2025-10-28T08:05:00Z',
          currentStep: {
            order: 2,
            component: 'grafana',
            action: 'upgrade',
            status: 'running',
            message: 'Upgrading Grafana',
          },
          components: [
            { name: 'prometheus', phase: 'Healthy', installedAt: '2025-10-28T08:05:50Z' },
            { name: 'grafana', phase: 'Installing' },
          ],
          completedSteps: [
            {
              order: 1,
              component: 'prometheus',
              action: 'install',
              status: 'succeeded',
              message: 'Installed successfully',
            },
          ],
          errors: [],
          checkpoint: {
            sessionId,
            phase: 'Installing',
            progress: 0.45,
            completedSteps: [1],
            currentStep: 2,
            startedAt: '2025-10-28T08:05:00Z',
            lastUpdatedAt: '2025-10-28T08:06:30Z',
          },
        },
        {
          sessionId,
          phase: 'Active',
          progress: 1,
          updatedAt: '2025-10-28T08:08:10Z',
          startedAt: '2025-10-28T08:05:00Z',
          currentStep: null,
          components: [
            { name: 'prometheus', phase: 'Healthy', installedAt: '2025-10-28T08:05:50Z' },
            { name: 'grafana', phase: 'Healthy', installedAt: '2025-10-28T08:07:55Z' },
          ],
          completedSteps: [
            {
              order: 1,
              component: 'prometheus',
              action: 'install',
              status: 'succeeded',
            },
            {
              order: 2,
              component: 'grafana',
              action: 'upgrade',
              status: 'succeeded',
            },
          ],
          errors: [],
          checkpoint: {
            sessionId,
            phase: 'Active',
            progress: 1,
            completedSteps: [1, 2],
            lastUpdatedAt: '2025-10-28T08:08:10Z',
          },
        },
      ],
    },
  };

  await setupMonitoringWizardMocks(page, scenario);
  await page.goto('/operations/monitoring/enable-wizard');

  await expect(page.getByRole('heading', { name: '监控安装向导' })).toBeVisible();
  await expect(page.getByText('Prometheus')).toBeVisible();

  await page.getByRole('button', { name: '继续下一步' }).click();
  const planTable = page.locator('.plan-table');
  await expect(planTable.locator('thead th').filter({ hasText: '组件' }).first()).toBeVisible();
  await page.getByRole('button', { name: '开始安装' }).click();

  await expect(page.getByText(`会话 ${sessionId}`)).toBeVisible();
  await expect(page.getByText('已完成')).toBeVisible({ timeout: 10_000 });
  await expect(page.locator('.component-list .component-item .comp-name').filter({ hasText: 'Grafana' }).first()).toBeVisible();
});

test('resume flow restores session from checkpoint', async ({ page }) => {
  const sessionId = 'sess-resume-existing';
  const resumeCheckpoint = {
    sessionId,
    phase: 'Installing',
    progress: 0.6,
    startedAt: '2025-10-27T13:40:00Z',
    lastUpdatedAt: '2025-10-27T13:55:00Z',
    components: [
      { name: 'prometheus', phase: 'Healthy' },
    ],
    errors: [],
    completedSteps: [1],
    currentStep: 2,
  };
  const now = Date.now();
  const resumeRecord = {
    key: 'polardbx-monitor',
    checkpoint: resumeCheckpoint,
    sessionId,
    sessions: [
      {
        sessionId,
        checkpoint: resumeCheckpoint,
        updatedAt: now,
      },
    ],
    updatedAt: now,
    expiresAt: now + 12 * 60 * 60 * 1000,
  };
  setAuthStorage(page, {
    script: ({ record }) => {
      const typedRecord = record as typeof resumeRecord;
      try {
        window.localStorage.setItem(`polardbx-monitoring-install:${typedRecord.key}`, JSON.stringify(typedRecord));
      } catch (error) {
        console.warn('failed to seed resume record in localStorage', error);
      }
      if (window.indexedDB) {
        try {
          const request = window.indexedDB.open('polardbx-monitoring-install', 1);
          request.onupgradeneeded = () => {
            const db = request.result;
            if (!db.objectStoreNames.contains('checkpoints')) {
              db.createObjectStore('checkpoints', { keyPath: 'key' });
            }
          };
          request.onsuccess = () => {
            try {
              const db = request.result;
              const tx = db.transaction('checkpoints', 'readwrite');
              tx.objectStore('checkpoints').put(typedRecord);
              tx.oncomplete = () => db.close();
            } catch (error) {
              console.warn('failed to seed resume record in indexedDB transaction', error);
            }
          };
        } catch (error) {
          console.warn('failed to seed resume record in indexedDB', error);
        }
      }
    },
    arg: { record: resumeRecord },
  });

  const scenario: MonitoringScenario = {
    detection: detectionSnapshot,
    statuses: {
      [sessionId]: [
        {
          sessionId,
          phase: 'Installing',
          progress: 0.6,
          updatedAt: '2025-10-27T13:55:10Z',
          startedAt: '2025-10-27T13:40:00Z',
          currentStep: {
            order: 2,
            component: 'grafana',
            action: 'install',
            status: 'running',
            message: 'Deploying Grafana',
          },
          components: [
            { name: 'prometheus', phase: 'Healthy', installedAt: '2025-10-27T13:42:00Z' },
            { name: 'grafana', phase: 'Planned' },
          ],
          completedSteps: [
            { order: 1, component: 'prometheus', action: 'install', status: 'succeeded' },
          ],
          errors: [],
          checkpoint: {
            sessionId,
            phase: 'Installing',
            progress: 0.6,
            completedSteps: [1],
            currentStep: 2,
            startedAt: '2025-10-27T13:40:00Z',
            lastUpdatedAt: '2025-10-27T13:55:10Z',
          },
        },
        {
          sessionId,
          phase: 'Active',
          progress: 1,
          updatedAt: '2025-10-27T13:57:40Z',
          startedAt: '2025-10-27T13:40:00Z',
          components: [
            { name: 'prometheus', phase: 'Healthy' },
            { name: 'grafana', phase: 'Healthy' },
          ],
          completedSteps: [
            { order: 1, component: 'prometheus', action: 'install', status: 'succeeded' },
            { order: 2, component: 'grafana', action: 'install', status: 'succeeded' },
          ],
          errors: [],
          checkpoint: {
            sessionId,
            phase: 'Active',
            progress: 1,
            completedSteps: [1, 2],
            lastUpdatedAt: '2025-10-27T13:57:40Z',
          },
        },
      ],
    },
  };

  await setupMonitoringWizardMocks(page, scenario);
  await page.goto('/operations/monitoring/enable-wizard');

  await expect(page.getByRole('heading', { name: '检测到未完成的安装流程' })).toBeVisible();
  await page.getByRole('button', { name: '继续' }).click();

  await expect(page.getByText(`会话 ${sessionId}`)).toBeVisible();
  await expect(page.getByText('已完成')).toBeVisible({ timeout: 10_000 });
});

test('partial failure can be diagnosed and auto-fixed', async ({ page }) => {
  setAuthStorage(page);

  const sessionId = 'sess-partial-failure';
  const failureStatus = {
    sessionId,
    phase: 'Failed',
    progress: 0.55,
    updatedAt: '2025-10-28T09:12:00Z',
    startedAt: '2025-10-28T09:05:00Z',
    currentStep: {
      order: 2,
      component: 'prometheus',
      action: 'install',
      status: 'failed',
      message: 'Pod pending timeout',
    },
    components: [
      { name: 'prometheus', phase: 'Failed', errorMessage: 'Waiting for PVC' },
      { name: 'grafana', phase: 'Healthy', installedAt: '2025-10-28T09:06:30Z' },
    ],
    completedSteps: [
      { order: 1, component: 'grafana', action: 'install', status: 'succeeded' },
    ],
    errors: [
      {
        category: 'Timeout',
        message: 'Prometheus StatefulSet 长时间未就绪',
        component: 'prometheus',
        stepOrder: 2,
        occurredAt: '2025-10-28T09:11:45Z',
        context: {
          pod: 'prometheus-0',
        },
      },
    ],
    retry: {
      mode: 'scheduled',
      retries: 1,
      maxRetries: 3,
      backoffSeconds: 120,
      nextRetryAt: '2025-10-28T09:14:00Z',
    },
    checkpoint: {
      sessionId,
      phase: 'Failed',
      progress: 0.55,
      completedSteps: [1],
      currentStep: 2,
      errors: [
        {
          category: 'Timeout',
          message: 'Prometheus StatefulSet 长时间未就绪',
          component: 'prometheus',
          stepOrder: 2,
        },
      ],
      startedAt: '2025-10-28T09:05:00Z',
      lastUpdatedAt: '2025-10-28T09:12:00Z',
    },
  };

  const successStatus = {
    sessionId,
    phase: 'Active',
    progress: 1,
    updatedAt: '2025-10-28T09:15:30Z',
    startedAt: '2025-10-28T09:05:00Z',
    components: [
      { name: 'prometheus', phase: 'Healthy', installedAt: '2025-10-28T09:14:30Z' },
      { name: 'grafana', phase: 'Healthy', installedAt: '2025-10-28T09:06:30Z' },
    ],
    completedSteps: [
      { order: 1, component: 'grafana', action: 'install', status: 'succeeded' },
      { order: 2, component: 'prometheus', action: 'install', status: 'succeeded' },
    ],
    errors: [],
    retry: {
      mode: 'idle',
      retries: 1,
      maxRetries: 3,
      backoffSeconds: 0,
    },
    checkpoint: {
      sessionId,
      phase: 'Active',
      progress: 1,
      completedSteps: [1, 2],
      lastUpdatedAt: '2025-10-28T09:15:30Z',
    },
  };

  const scenario: MonitoringScenario = {
    detection: detectionSnapshot,
    plan: {
      plan: {
        sessionTemplate: {
          namespace: 'polardbx-monitor',
          releaseName: 'polardbx-monitor',
          intent: 'install',
          targetNamespace: 'polardbx-monitor',
        },
        riskLevel: 'high',
        steps: [
          { order: 1, component: 'grafana', action: 'install', estimatedDurationSeconds: 180, dependsOn: [] },
          { order: 2, component: 'prometheus', action: 'install', estimatedDurationSeconds: 420, dependsOn: [1] },
        ],
      },
      estimatedDurationSeconds: 600,
    },
    start: {
      sessionId,
      namespace: 'polardbx-monitor',
      createdAt: '2025-10-28T09:05:00Z',
      resumed: false,
    },
    statuses: {
      [sessionId]: [failureStatus],
    },
    afterAutoFixStatuses: {
      [sessionId]: [successStatus],
    },
    diagnose: {
      diagnosis: {
        category: 'PodPending',
        severity: 'major',
        summary: 'Prometheus Pod 长时间处于 Pending 状态',
        possibleCauses: ['PVC is not bound to a storage resource', 'Insufficient node resources (CPU/memory)'],
        suggestedFixes: ['Check the StorageClass configuration for the Prometheus data volume', 'Ensure the cluster has enough node resources'],
      },
      autoFixes: [
        {
          id: 'monitoring::prometheus::restart',
          title: 'Restart Prometheus StatefulSet',
          description: 'Trigger a rolling restart to reschedule the Pod',
          verification: 'Confirm the latest Pod Ready status is True',
          automated: false,
        },
      ],
    },
    autoFixResponses: {
      'monitoring::prometheus::restart': {
        success: true,
        message: '已触发 Prometheus 滚动重启',
      },
    },
  };

  await setupMonitoringWizardMocks(page, scenario);
  await page.goto('/operations/monitoring/enable-wizard');

  await page.getByRole('button', { name: '继续下一步' }).click();
  await page.getByRole('button', { name: '开始安装' }).click();

  await expect(page.locator('.ant-tag').filter({ hasText: '失败' }).first()).toBeVisible();
  await expect(page.getByText('Prometheus StatefulSet 长时间未就绪')).toBeVisible();

  await page.getByRole('button', { name: '打开诊断' }).click();
  await expect(page.getByText('Prometheus Pod 长时间处于 Pending 状态')).toBeVisible();

  await page.getByRole('tab', { name: '自动修复' }).click();
  await page.getByRole('button', { name: '执行修复' }).click();
  await expect(page.getByText('已触发 Prometheus 滚动重启')).toBeVisible();
  await expect(page.getByText('已完成')).toBeVisible({ timeout: 10_000 });
});
