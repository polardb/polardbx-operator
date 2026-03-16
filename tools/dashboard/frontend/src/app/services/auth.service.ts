import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { BehaviorSubject } from 'rxjs';

export interface SessionInfo { enabled: boolean; username?: string; role?: string; expiresAt?: string; }

@Injectable({ providedIn: 'root' })
export class AuthService {
  private http = inject(HttpClient);
  private baseUrl = '/api/v1';

  private sessionSubject = new BehaviorSubject<SessionInfo>({ enabled: false });
  session$ = this.sessionSubject.asObservable();

  get token(): string | null { return localStorage.getItem('jwtToken'); }

  async init(): Promise<void> { await this.refreshSession(); }

  async refreshSession(): Promise<void> {
    try {
      const headers = new HttpHeaders(this.token ? { Authorization: `Bearer ${this.token}` } : {});
      const me: any = await this.http.get(`${this.baseUrl}/auth/me`, { headers }).toPromise();
      if (me?.enabled) {
        this.sessionSubject.next({ enabled: true, username: me.username, role: me.role, expiresAt: me.exp });
      } else {
        this.sessionSubject.next({ enabled: false });
      }
    } catch (e: any) {
      // If backend enabled JWT, unauthorized returns {enabled:true,...}
      const errBody = e?.error || {};
      if (errBody?.enabled) {
        this.sessionSubject.next({ enabled: true });
      } else {
        this.sessionSubject.next({ enabled: false });
      }
    }
  }

  async login(username: string, password: string): Promise<SessionInfo> {
    const headers = new HttpHeaders({ 'Content-Type': 'application/json' });
    const resp: any = await this.http.post(`${this.baseUrl}/auth/login`, { username, password }, { headers }).toPromise();
    const token = resp?.token as string;
    if (token) {
      localStorage.setItem('jwtToken', token);
      await this.refreshSession();
    }
    return this.sessionSubject.value;
  }

  logout(): void {
    localStorage.removeItem('jwtToken');
    this.sessionSubject.next({ enabled: this.sessionSubject.value.enabled });
  }

  // --- Kubeconfig-based access helpers (legacy, still supported) ---
  saveKubeconfig(kubeconfig: string): void {
    localStorage.setItem('kubeconfig', kubeconfig);
    // 额外缓存 base64 供 SSE 查询参数使用（SSE 无法自定义 Header）
    try {
      const b64 = btoa(unescape(encodeURIComponent(kubeconfig)));
      localStorage.setItem('kubeconfig-b64', b64);
    } catch {}
  }

  getKubeconfig(): string | null {
    return localStorage.getItem('kubeconfig');
  }

  clearKubeconfig(): void {
    localStorage.removeItem('kubeconfig');
    localStorage.removeItem('kubeconfig-b64');
  }

  isAuthenticated(): boolean {
    return !!this.getKubeconfig();
  }
}