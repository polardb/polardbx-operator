import type { components } from './generated/monitoring-installation';

export type MonitoringErrorCode = components['schemas']['ErrorCode'];

export const MonitoringErrorCodes = {
  DetectUnavailable: 'monitoring/detect-unavailable',
  DetectFailed: 'monitoring/detect-failed',
  PlanInvalid: 'monitoring/plan-invalid',
  InstallPlanEmpty: 'monitoring/install-plan-empty',
  InstallInvalidRequest: 'monitoring/install-invalid-request',
  KubernetesClientMissing: 'monitoring/k8s-client-missing',
  SessionCreateFailed: 'monitoring/session-create-failed',
  SessionRequired: 'monitoring/session-required',
  SessionPersistFailed: 'monitoring/session-persist-failed',
  SessionRestoreFailed: 'monitoring/session-restore-failed',
  SessionNotFound: 'monitoring/session-not-found',
  RetryInvalidRequest: 'monitoring/retry-invalid-request',
  RetryInvalidMode: 'monitoring/retry-invalid-mode',
  RetryActivateFailed: 'monitoring/retry-activate-failed',
  RetryPersistFailed: 'monitoring/retry-persist-failed',
  RetryUnavailable: 'monitoring/retry-unavailable',
  RetryLimitReached: 'monitoring/retry-limit-reached',
  RetryStartFailed: 'monitoring/retry-start-failed',
  DiagnoseInvalid: 'monitoring/diagnose-invalid',
  DiagnoseContextFailed: 'monitoring/diagnose-context-failed',
  DiagnoseFailed: 'monitoring/diagnose-failed',
  AutoFixInvalid: 'monitoring/autofix-invalid',
  AutoFixContextFailed: 'monitoring/autofix-context-failed',
  AutoFixFailed: 'monitoring/autofix-failed',
  Unknown: 'monitoring/unknown-error',
} satisfies Record<string, MonitoringErrorCode>;

export const MonitoringErrorCodeList: MonitoringErrorCode[] = Object.values(MonitoringErrorCodes);
