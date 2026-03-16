import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import type { components } from '../models/generated/monitoring-installation';
import { buildJsonHeaders } from '../utils/http-headers';

export type EnvironmentSnapshot = components['schemas']['EnvironmentSnapshot'];
export type CreatePlanRequest = components['schemas']['CreatePlanRequest'];
export type CreatePlanResponse = components['schemas']['CreatePlanResponse'];
export type StartInstallRequest = components['schemas']['StartInstallRequest'];
export type StartInstallResponse = components['schemas']['StartInstallResponse'];
export type InstallStatusResponse = components['schemas']['InstallStatusResponse'];
export type DiagnoseRequest = components['schemas']['DiagnoseRequest'];
export type DiagnoseResponse = components['schemas']['DiagnoseResponse'];
export type AutoFixRequest = components['schemas']['AutoFixRequest'];
export type AutoFixResponse = components['schemas']['AutoFixResponse'];
export type RetryStatus = components['schemas']['RetryStatus'];
export type RetryRequest = components['schemas']['RetryRequest'];

@Injectable({ providedIn: 'root' })
export class MonitoringInstallationApiService {
  private readonly baseUrl = '/api/v1';

  private readonly http = inject(HttpClient);

  private getHeaders(): HttpHeaders {
    return buildJsonHeaders();
  }

  detectEnvironment(namespace: string): Observable<EnvironmentSnapshot> {
    const params = new HttpParams().set('namespace', namespace);
    return this.http.get<EnvironmentSnapshot>(`${this.baseUrl}/monitoring/detect`, {
      params,
      headers: this.getHeaders()
    });
  }

  createPlan(body: CreatePlanRequest): Observable<CreatePlanResponse> {
    return this.http.post<CreatePlanResponse>(`${this.baseUrl}/monitoring/plan`, body, {
      headers: this.getHeaders()
    });
  }

  startInstallation(body: StartInstallRequest): Observable<StartInstallResponse> {
    return this.http.post<StartInstallResponse>(`${this.baseUrl}/monitoring/install`, body, {
      headers: this.getHeaders()
    });
  }

  getInstallStatus(sessionId: string): Observable<InstallStatusResponse> {
    return this.http.get<InstallStatusResponse>(`${this.baseUrl}/monitoring/install/${encodeURIComponent(sessionId)}/status`, {
      headers: this.getHeaders()
    });
  }

  triggerRetry(sessionId: string, body?: RetryRequest): Observable<InstallStatusResponse> {
    const payload = body ?? {};
    return this.http.post<InstallStatusResponse>(`${this.baseUrl}/monitoring/install/${encodeURIComponent(sessionId)}/retry`, payload, {
      headers: this.getHeaders()
    });
  }

  diagnoseFailure(body: DiagnoseRequest): Observable<DiagnoseResponse> {
    return this.http.post<DiagnoseResponse>(`${this.baseUrl}/monitoring/diagnose`, body, {
      headers: this.getHeaders()
    });
  }

  applyAutoFix(body: AutoFixRequest): Observable<AutoFixResponse> {
    return this.http.post<AutoFixResponse>(`${this.baseUrl}/monitoring/auto-fix`, body, {
      headers: this.getHeaders()
    });
  }
}
