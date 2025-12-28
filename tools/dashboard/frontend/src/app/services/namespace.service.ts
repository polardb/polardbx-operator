import { Injectable, inject } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { ApiService } from './api.service';

const STORAGE_KEY = 'activeNamespace';

@Injectable({ providedIn: 'root' })
export class NamespaceService {
  private api = inject(ApiService);

  private activeSubject = new BehaviorSubject<string | null>(null);
  private listSubject = new BehaviorSubject<string[]>([]);

  activeNamespace$ = this.activeSubject.asObservable();
  namespaces$ = this.listSubject.asObservable();

  async init(): Promise<void> {
    let def = 'polardbx-operator-system';
    try {
      const ctx = await this.api.getSystemContext().toPromise();
      const nsFromStorage = localStorage.getItem(STORAGE_KEY);
      def = nsFromStorage || ctx?.defaultNamespace || def;
    } catch {
      const nsFromStorage = localStorage.getItem(STORAGE_KEY);
      def = nsFromStorage || def;
    }
    this.setActive(def);

    try {
      const ns = await this.api.listSystemNamespaces().toPromise();
      const items = (ns?.items || []).map(x => x.name);
      this.listSubject.next(items);
    } catch {
      this.listSubject.next([]);
    }
  }

  setActive(ns: string): void {
    this.activeSubject.next(ns);
    localStorage.setItem(STORAGE_KEY, ns);
  }

  getActiveSync(): string | null {
    return this.activeSubject.value;
  }
}
