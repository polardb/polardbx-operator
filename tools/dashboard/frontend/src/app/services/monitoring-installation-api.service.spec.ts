import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import {
  MonitoringInstallationApiService,
  type AutoFixResponse,
  type CreatePlanRequest,
  type CreatePlanResponse,
  type DiagnoseRequest,
  type DiagnoseResponse,
  type EnvironmentSnapshot,
  type InstallStatusResponse,
  type RetryRequest,
  type StartInstallRequest,
  type StartInstallResponse
} from './monitoring-installation-api.service';

describe('MonitoringInstallationApiService', () => {
  let service: MonitoringInstallationApiService;
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule]
    });
    service = TestBed.inject(MonitoringInstallationApiService);
    http = TestBed.inject(HttpTestingController);
    localStorage.clear();
  });

  afterEach(() => {
    http.verify();
  });

  it('detectEnvironment issues GET with namespace query', () => {
    const namespace = 'polardbx-monitor';
    let response: EnvironmentSnapshot | undefined;

    service.detectEnvironment(namespace).subscribe(res => {
      response = res;
    });

    const req = http.expectOne(request => request.method === 'GET' && request.url === '/api/v1/monitoring/detect');
    expect(req.request.params.get('namespace')).toBe(namespace);
    expect(req.request.headers.get('Content-Type')).toBe('application/json');

    const payload = { namespace, components: [], detectedAt: new Date().toISOString() } as EnvironmentSnapshot;
    req.flush(payload);

    expect(response).toEqual(payload);
  });

  it('createPlan posts payload to /monitoring/plan', () => {
    const body: CreatePlanRequest = {
      config: {
        installMode: 'assisted'
      },
      detected: {
        namespace: 'polardbx-monitor',
        detectedAt: new Date().toISOString(),
        components: []
      }
    };

    service.createPlan(body).subscribe();

    const req = http.expectOne('/api/v1/monitoring/plan');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(body);

    const result: CreatePlanResponse = {
      plan: {
        sessionTemplate: {
          namespace: 'polardbx-monitor',
          intent: 'install',
          targetNamespace: 'polardbx-monitor'
        },
        steps: []
      },
      estimatedDurationSeconds: 0
    };
    req.flush(result);
  });

  it('startInstallation posts to /monitoring/install', () => {
    const payload: StartInstallRequest = {
      plan: {
        sessionTemplate: {
          namespace: 'polardbx-monitor'
        },
        steps: []
      }
    };
    service.startInstallation(payload).subscribe();

    const req = http.expectOne('/api/v1/monitoring/install');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(payload);

    const res: StartInstallResponse = {
      sessionId: 'sess',
      namespace: 'polardbx-monitor',
      createdAt: new Date().toISOString()
    };
    req.flush(res);
  });

  it('getInstallStatus encodes session id path segment', () => {
    const sessionId = 'session/with special';
    let status: InstallStatusResponse | undefined;

    service.getInstallStatus(sessionId).subscribe(res => {
      status = res;
    });

    const expectedUrl = `/api/v1/monitoring/install/${encodeURIComponent(sessionId)}/status`;
    const req = http.expectOne(expectedUrl);
    expect(req.request.method).toBe('GET');

    const payload: InstallStatusResponse = {
      sessionId,
      phase: 'Installing',
      components: [],
      updatedAt: new Date().toISOString()
    };
    req.flush(payload);

    expect(status).toEqual(payload);
  });

  it('triggerRetry posts default empty object when body omitted', () => {
    const sessionId = 'retry-session';
    service.triggerRetry(sessionId).subscribe();

    const req = http.expectOne(`/api/v1/monitoring/install/${encodeURIComponent(sessionId)}/retry`);
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({});

    const resp: InstallStatusResponse = {
      sessionId,
      phase: 'Installing',
      components: [],
      updatedAt: new Date().toISOString()
    };
    req.flush(resp);
  });

  it('triggerRetry posts provided payload when body specified', () => {
    const sessionId = 'retry-session';
    const body: RetryRequest = { mode: 'manual', reason: 'force' };

    service.triggerRetry(sessionId, body).subscribe();

    const req = http.expectOne(`/api/v1/monitoring/install/${encodeURIComponent(sessionId)}/retry`);
    expect(req.request.body).toEqual(body);
    const retryResponse: InstallStatusResponse = {
      sessionId,
      phase: 'Installing',
      components: [],
      updatedAt: new Date().toISOString()
    };
    req.flush(retryResponse);
  });

  it('diagnoseFailure posts to /monitoring/diagnose', () => {
    const requestBody = { error: { category: 'Unknown', message: 'failure' } } as DiagnoseRequest;
    let response: DiagnoseResponse | undefined;

    service.diagnoseFailure(requestBody).subscribe(res => {
      response = res;
    });

    const req = http.expectOne('/api/v1/monitoring/diagnose');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(requestBody);

    const diagnose = { diagnosis: { category: 'Unknown', severity: 'minor', summary: 'failure' } } as DiagnoseResponse;
    req.flush(diagnose);

    expect(response).toEqual(diagnose);
  });

  it('applyAutoFix posts to /monitoring/auto-fix', () => {
    const requestBody = { fixId: 'monitoring::prometheus::restart', sessionId: 'sess' };
    let response: AutoFixResponse | undefined;

    service.applyAutoFix(requestBody).subscribe(res => {
      response = res;
    });

    const req = http.expectOne('/api/v1/monitoring/auto-fix');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(requestBody);

    const autoFix = { success: true } as AutoFixResponse;
    req.flush(autoFix);

    expect(response).toEqual(autoFix);
  });
});
