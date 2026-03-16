import { AfterViewInit, OnInit, OnChanges, OnDestroy, SimpleChanges, Component, ElementRef, ViewChild, inject, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { NzModalRef, NZ_MODAL_DATA } from 'ng-zorro-antd/modal';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';

import { Terminal } from 'xterm';
import { FitAddon } from '@xterm/addon-fit';

// Type definitions for terminal interfaces  
interface XTerminal {
  open(element: HTMLElement): void;
  attachCustomKeyEventHandler?(handler: (ev: KeyboardEvent) => boolean): void;
  focus?(): void;
  onData?(callback: (data: string) => void): void;
  write?(text: string): void;
  writeln?(text: string): void;
  clear?(): void;
  getSelection?(): string;
  _dataHandlerBound?: boolean;
}

interface SimpleTerminal {
  writeln(text: string): void;
  write(text: string): void;
  clear(): void;
  focus(): void;
  onData(callback: (data: string) => void): void;
  _onDataCallback?: (data: string) => void;
  _dataHandlerBound?: boolean;
}

export interface WebShellDialogData {
  namespace: string;
  pod: string;
  container?: string;
  containers?: string[];
}

@Component({
  selector: 'app-webshell-dialog',
  template: `
  <div class="modal-header" *ngIf="!embedded">
    <i nz-icon nzType="desktop" class="modal-title-icon"></i>
    <span class="modal-title-text">WebShell</span>
  </div>
  <div class="modal-body">
    <div class="toolbar">
      <i nz-icon nzType="cluster"></i>
      <span class="kv">ns</span><span class="vv">{{ data.namespace }}</span>
      <span class="kv">pod</span><span class="vv">{{ data.pod }}</span>
      <span class="kv">容器</span>
      <nz-select
        [(ngModel)]="currentContainer"
        (ngModelChange)="onContainerChange($event)"
        class="container-select"
        [nzDisabled]="connecting">
        <nz-option *ngFor="let c of containerList" [nzValue]="c" [nzLabel]="c"></nz-option>
      </nz-select>
      <span class="spacer"></span>
      <button nz-button nzType="default" nzShape="circle" nz-tooltip nzTooltipTitle="复制" (click)="copySelection()" [disabled]="connecting">
        <i nz-icon nzType="copy"></i>
      </button>
      <button nz-button nzType="default" nzShape="circle" nz-tooltip nzTooltipTitle="粘贴" (click)="pasteFromClipboard()" [disabled]="connecting">
        <i nz-icon nzType="file-text"></i>
      </button>
      <button nz-button nzType="default" nzShape="circle" nz-tooltip nzTooltipTitle="清屏 (Ctrl+L)" (click)="clearScreen()">
        <i nz-icon nzType="delete"></i>
      </button>
      <button nz-button nzType="default" nzShape="circle" nz-tooltip nzTooltipTitle="下载输出" (click)="downloadLog()">
        <i nz-icon nzType="download"></i>
      </button>
      <button nz-button nzType="default" (click)="reconnect()" [nzLoading]="connecting">
        <i nz-icon nzType="reload"></i>
        <span>重连</span>
      </button>
    </div>
    <div #term class="terminal" [ngClass]="embedded ? 'terminal-embedded' : 'terminal-modal'"></div>
    <div class="tips">
      快捷键：Ctrl+C 结束进程；Ctrl+L 清屏；如需复制/粘贴请使用上方按钮（浏览器权限限制）。
    </div>
  </div>
  <div class="modal-footer" *ngIf="!embedded">
    <button nz-button nzType="default" (click)="onClose()">关闭</button>
  </div>
  `,
  styles: [`
    .modal-header {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 16px 4px 16px;
      font-size: 16px;
      font-weight: 600;
      border-bottom: 1px solid #f0f0f0;
    }
    .modal-title-icon {
      font-size: 18px;
      color: #1890ff;
    }
    .modal-title-text {
      flex: 1;
    }
    .modal-body { padding: 8px 16px 12px 16px; }
    .modal-footer {
      padding: 8px 16px 12px 16px;
      text-align: right;
      border-top: 1px solid #f0f0f0;
    }
    .toolbar { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; font-size: 12px; }
    .kv { color: #666; margin-left: 8px; }
    .vv { font-weight: 600; margin-right: 12px; }
    .spacer { flex: 1; }
    .container-select { width: 180px; }
    .terminal { width: 100%; background: #0b1020; border-radius: 6px; }
    .terminal-modal { height: 70vh; min-height: 380px; }
    .terminal-embedded { height: 60vh; min-height: 420px; }
    .tips { margin-top: 8px; font-size: 12px; color: #666; }
  `],
  standalone: true,
  imports: [CommonModule, FormsModule, NzButtonModule, NzIconModule, NzSelectModule, NzToolTipModule]
})
export class WebShellDialogComponent implements OnInit, OnChanges, AfterViewInit, OnDestroy {
  @Input() embedded = false;
  @Input() namespace?: string;
  @Input() pod?: string;
  @Input() container?: string;
  @Input() containers?: string[];

  // Supports being opened via MatDialog or NzModalService.
  dialogRef = inject<MatDialogRef<WebShellDialogComponent> | null>(MatDialogRef, { optional: true });
  nzModalRef = inject<NzModalRef<WebShellDialogComponent> | null>(NzModalRef as any, { optional: true });
  // Compatible with both MatDialog (MAT_DIALOG_DATA) and NzModal (NZ_MODAL_DATA) data sources.
  private matDialogData = inject<WebShellDialogData | null>(MAT_DIALOG_DATA as any, { optional: true });
  private nzModalData = inject<WebShellDialogData | null>(NZ_MODAL_DATA, { optional: true });
  data: WebShellDialogData = { namespace: '', pod: '' };
  @ViewChild('term', { static: false }) termRef!: ElementRef<HTMLDivElement>;
  private term: any;
  private fitAddon?: FitAddon;
  private ws?: WebSocket;
  private sessionLog = '';
  containerList: string[] = [];
  currentContainer = '';
  connecting = false;
  private fallbackTried = false;
  private pendingInput = '';
  private suppressAutoReconnect = false;
  private resizeObserver?: ResizeObserver;
  private readonly onWindowResize = () => this.handleResize();
  private destroyed = false;

  private close(result?: any): void {
    this.suppressAutoReconnect = true;
    try {
      if (this.term && typeof this.term.dispose === 'function') {
        this.term.dispose();
      } else if (this.term && typeof this.term.clear === 'function') {
        this.term.clear();
      }
    } catch {}
    try { this.ws?.close(); } catch {}
    if (this.dialogRef) {
      this.dialogRef.close(result);
    } else if (this.nzModalRef) {
      this.nzModalRef.close(result);
    }
  }

  ngOnInit(): void {
    this.resolveDataFromInputsOrInjection();
    this.bindContainerListFromData();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['namespace'] || changes['pod'] || changes['containers'] || changes['container']) {
      this.resolveDataFromInputsOrInjection();
      this.bindContainerListFromData();
      if (this.termRef) {
        this.reconnect();
      }
    }
  }

  ngOnDestroy(): void {
    this.destroyed = true;
    this.suppressAutoReconnect = true;
    try { window.removeEventListener('resize', this.onWindowResize); } catch {}
    try { this.resizeObserver?.disconnect(); } catch {}
    try { this.ws?.close(); } catch {}
    try { this.term?.dispose?.(); } catch {}
  }

  ngAfterViewInit(): void {
    if (!this.data.namespace || !this.data.pod) {
      this.initSimpleTerminal();
      this.term?.writeln('[错误] 缺少 namespace / pod 参数，无法打开终端');
      return;
    }

    try {
      this.term = new Terminal({
        fontFamily: 'Menlo, Monaco, Consolas, "Courier New", monospace',
        fontSize: 13,
        theme: { background: '#0b1020' },
        cursorBlink: true,
        convertEol: true,
        scrollback: 2000
      });
      this.fitAddon = new FitAddon();
      this.term.loadAddon?.(this.fitAddon);
      this.term.open(this.termRef.nativeElement);
      this.fitAddon?.fit();
      this.term.attachCustomKeyEventHandler?.((ev: KeyboardEvent) => {
        if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === 'l') { this.clearScreen(); return false; }
        return true;
      });

      // Bind terminal input immediately; buffer until WS ready.
      this.term.onData((data: string) => {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.sendStdin(data);
        } else {
          this.pendingInput += data;
        }
      });
      (this.term as any)._dataHandlerBound = true;

      this.termRef.nativeElement.addEventListener('click', () => this.term?.focus?.());
      // Focus the terminal immediately.
      this.term.focus?.();

      window.addEventListener('resize', this.onWindowResize);
      this.resizeObserver = new ResizeObserver(() => this.handleResize());
      this.resizeObserver.observe(this.termRef.nativeElement);
      // Fit again after first paint (tab switch / modal animation).
      setTimeout(() => this.handleResize(), 0);
    } catch {
      this.initSimpleTerminal();
    }
    setTimeout(() => this.connectWebSocket());
  }

  private initSimpleTerminal(): void {
    const termDiv = this.termRef.nativeElement;
    termDiv.style.fontFamily = 'Menlo, Monaco, Consolas, "Courier New", monospace';
    termDiv.style.fontSize = '13px';
    termDiv.style.color = '#d6e4ff';
    termDiv.style.backgroundColor = '#0b1020';
    termDiv.style.padding = '12px';
    termDiv.style.overflow = 'auto';
    termDiv.style.whiteSpace = 'pre-wrap';
    termDiv.style.wordBreak = 'break-all';
    termDiv.style.height = '100%';
    termDiv.innerHTML = '[等待连接...]';
    
    // Create a minimal terminal wrapper.
    this.term = {
      writeln: (text: string) => {
        termDiv.innerHTML += text + '\n';
        termDiv.scrollTop = termDiv.scrollHeight;
      },
      write: (text: string) => {
        // Handle common terminal control sequences.
        let processed = text;
        
        // Handle backspace: \b or \u0008
        processed = processed.replace(/\u0008/g, () => {
          // Remove the last displayed character.
          const current = termDiv.textContent || '';
          if (current.length > 0) {
            termDiv.textContent = current.slice(0, -1);
          }
          return '';
        });
        
        // Handle carriage return.
        processed = processed.replace(/\r/g, '\n');
        
        // Handle ANSI color codes and control sequences.
        processed = this.parseAnsiColors(processed);
        
        // Append remaining content.
        if (processed) {
          termDiv.innerHTML += processed;
        }
        
        termDiv.scrollTop = termDiv.scrollHeight;
      },
      clear: () => {
        termDiv.innerHTML = '';
      },
      focus: () => {
        termDiv.focus();
      },
      onData: (callback: (data: string) => void) => {
        // Store the callback and invoke it after the WebSocket is ready.
        this.term._onDataCallback = callback;

        // Debounce duplicate input.
        let lastKeyTime = 0;
        let lastKey = '';

        // Minimal keyboard input handling (consistent with the inline component).
        const handler = (e: KeyboardEvent) => {
          // Only handle keys when the terminal is focused to avoid interfering with the page.
          if (document.activeElement !== termDiv) {
            return;
          }

          const now = Date.now();
          const keyIdentifier = e.key + (e.ctrlKey ? '+ctrl' : '') + (e.altKey ? '+alt' : '');

          // Debounce: handle the same key at most once within 100ms.
          if (keyIdentifier === lastKey && now - lastKeyTime < 100) {
            e.preventDefault();
            e.stopPropagation();
            return;
          }

          lastKey = keyIdentifier;
          lastKeyTime = now;

          let payload = '';
          if (e.ctrlKey && !e.altKey && !e.metaKey) {
            const k = e.key.toLowerCase();
            const ctrlMap: Record<string, string> = {
              'c': '\u0003', 'd': '\u0004', 'z': '\u001a', 'a': '\u0001', 'e': '\u0005',
              'k': '\u000b', 'u': '\u0015', 'w': '\u0017', 'r': '\u0012', 'l': '\u000c'
            };
            if (ctrlMap[k]) payload = ctrlMap[k];
          }
          if (!payload) {
            switch (e.key) {
              case 'Enter': payload = '\r'; break;
              case 'Backspace': payload = '\u0008'; break; // Switch back to BS.
              case 'Tab': payload = '\t'; break;
              case 'ArrowUp': payload = '\u001b[A'; break;
              case 'ArrowDown': payload = '\u001b[B'; break;
              case 'ArrowRight': payload = '\u001b[C'; break;
              case 'ArrowLeft': payload = '\u001b[D'; break;
              case 'Delete': payload = '\u001b[3~'; break;
              case 'Home': payload = '\u001b[H'; break;
              case 'End': payload = '\u001b[F'; break;
              case 'PageUp': payload = '\u001b[5~'; break;
              case 'PageDown': payload = '\u001b[6~'; break;
              default:
                if (e.key.length === 1 && !e.metaKey) payload = e.key;
            }
          }
          if (payload) {
            const cb = (this.term && this.term._onDataCallback) || callback;
            if (cb) {
              cb(payload);
            }
            e.preventDefault();
            e.stopPropagation();
          }
        };

        // Listen on both the terminal element and window as a fallback for better compatibility.
        termDiv.addEventListener('keydown', handler);
        window.addEventListener('keydown', handler);
      }
    };

    // Bind input -> WS (same behavior as xterm): buffer until WS ready.
    try {
      this.term.onData((data: string) => {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.sendStdin(data);
        } else {
          this.pendingInput += data;
        }
      });
      (this.term as any)._dataHandlerBound = true;
    } catch {}
    
    // Make the terminal focusable.
    termDiv.tabIndex = 0;
    termDiv.setAttribute('role', 'textbox');
    termDiv.setAttribute('aria-multiline', 'true');
    termDiv.addEventListener('click', () => termDiv.focus());
    termDiv.focus();
  }

  onClose(): void {
    this.close();
  }

  onContainerChange(_c: string): void {
    // Allow auto-fallback again after manual container switch.
    this.fallbackTried = false;
    this.reconnect();
  }

  reconnect(): void {
    this.suppressAutoReconnect = true;
    try { this.ws?.close(); } catch {}
    this.suppressAutoReconnect = false;
    this.connectWebSocket();
  }

  private connectWebSocket(): void {
    const kubeconfig = localStorage.getItem('kubeconfig') || '';
    if (!kubeconfig) {
      this.term?.writeln('未找到 kubeconfig，请先连接 Kubernetes 集群。');
      return;
    }
    const kubeconfigB64 = btoa(unescape(encodeURIComponent(kubeconfig)));
    const wsScheme = (window.location?.protocol || 'http:') === 'https:' ? 'wss' : 'ws';
    // Use the same host:port as the frontend (same image/domain deployments and local proxy).
    const backendHost = window.location.host;
    const ns = encodeURIComponent(this.data.namespace);
    const pod = encodeURIComponent(this.data.pod);
    const container = encodeURIComponent(this.currentContainer || this.pickBestContainerFromList(this.containerList, this.data.container) || 'engine');
    const url = `${wsScheme}://${backendHost}/api/v1/platform/pods/${ns}/${pod}/exec?container=${container}&k=${encodeURIComponent(kubeconfigB64)}`;

    this.connecting = true;
    try {
      this.ws = new WebSocket(url);
    } catch (e) {
      this.term?.writeln(`WebSocket 连接失败: ${(e as any)?.message || e}`);
      this.connecting = false;
      return;
    }

    this.ws.binaryType = 'arraybuffer';
    this.ws.onopen = () => {
      this.connecting = false;
      this.fallbackTried = false;
      this.term?.writeln(`[已连接] 容器=${this.currentContainer || '-'}，按 Ctrl+C 结束进程`);
      this.term?.focus();
      this.sessionLog = '';

      // Sync terminal size (backend supports resize op).
      this.sendResize();

      // Flush buffered input if any.
      if (this.pendingInput) {
        this.sendStdin(this.pendingInput);
        this.pendingInput = '';
      }
    };
    this.ws.onmessage = (ev) => {
      const data = ev.data;
      let text = '';
      if (typeof data === 'string') {
        text = data;
      } else if (data instanceof ArrayBuffer) {
        text = new TextDecoder().decode(new Uint8Array(data));
      } else if (data && typeof (data as any).arrayBuffer === 'function') {
        (data as Blob).arrayBuffer().then(buf => {
          const t = new TextDecoder().decode(new Uint8Array(buf));
          this.handleIncomingStreamText(t);
        }).catch(() => {});
        return;
      }
      this.handleIncomingStreamText(text);
    };
    this.ws.onerror = () => {
      this.term?.writeln('\r\n[错误] WebSocket 通道异常');
    };
    this.ws.onclose = () => {
      this.term?.writeln('\r\n[断开] 会话已结束');
      this.connecting = false;
      setTimeout(() => {
        if (this.destroyed || this.suppressAutoReconnect) {
          return;
        }
        if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
          this.term?.writeln('[重试] 正在重连...');
          this.connectWebSocket();
        }
      }, 1500);
    };
  }

  private resolveDataFromInputsOrInjection(): void {
    const injected = this.matDialogData || this.nzModalData;
    if (injected) {
      this.data = injected;
      return;
    }
    this.data = {
      namespace: this.namespace || '',
      pod: this.pod || '',
      container: this.container,
      containers: this.containers
    };
  }

  private bindContainerListFromData(): void {
    this.containerList = this.data.containers || (this.data.container ? [this.data.container] : []);
    this.currentContainer = this.pickBestContainerFromList(this.containerList, this.data.container);
  }

  private b64Encode(text: string): string {
    return btoa(unescape(encodeURIComponent(text)));
  }

  private sendStdin(text: string): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    try {
      this.ws.send(JSON.stringify({ op: 'stdin', data: this.b64Encode(text) }));
    } catch (e) {
      console.error('WebSocket send error:', e);
    }
  }

  private handleResize(): void {
    try {
      this.fitAddon?.fit();
      this.sendResize();
    } catch {}
  }

  private sendResize(): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const rows = (this.term as any)?.rows || 24;
    const cols = (this.term as any)?.cols || 80;
    try {
      this.ws.send(JSON.stringify({ op: 'resize', rows, cols }));
    } catch {}
  }

  /**
   * Handle stream data returned by backend exec WebSocket.
   *
   * Supports two formats:
   * 1) Plain text stream
   * 2) JSON frames: {"data":"BASE64_OR_TEXT","op":"stdout"} (objects may be concatenated)
   */
  private handleIncomingStreamText(raw: string): void {
    if (!raw) return;

    // Intercept container-not-found errors first.
    if (this.handleContainerNotFound(raw)) return;

    const trimmed = raw.trim();

    // If it looks like a JSON frame, try to parse it.
    if (trimmed.startsWith('{') && trimmed.includes('"op"')) {
      // Handle concatenated JSON objects: }{ -> }@@SPLIT@@{
      const parts = trimmed
        .replace(/}\s*{/g, '}@@SPLIT@@{')
        .split('@@SPLIT@@');

      for (const part of parts) {
        try {
          const obj = JSON.parse(part);
          const op = obj?.op;
          let data = obj?.data;
          if (typeof data !== 'string') {
            continue;
          }

          // Some implementations base64-encode `data`.
          try {
            const decoded = atob(data);
            data = decoded;
          } catch {
            // Not valid base64; treat as plain text.
          }

          if (op === 'stdout' || op === 'stderr') {
            this.appendOutput(data);
          }
          // Ignore other ops (resize, ping, ...).
        } catch {
          // Fall back to plain text output when parsing fails.
          this.appendOutput(part);
        }
      }
      return;
    }

    // Default: plain text.
    this.appendOutput(raw);
  }

  // If we hit "container not found (\"xxx\")" or "unable to upgrade connection: container not found",
  // automatically select an existing container and reconnect (at most once per connection).
  private handleContainerNotFound(message: string): boolean {
    const m = message || '';
    if (this.fallbackTried) return false;
    if (/container not found/i.test(m) || /unable to upgrade connection: container not found/i.test(m)) {
      const fallback = this.pickBestContainer();
      if (fallback && fallback !== this.currentContainer) {
        this.fallbackTried = true;
        this.term?.writeln(`\r\n[提示] 容器 ${this.currentContainer} 不存在，自动切换到 ${fallback} 并重连...`);
        this.currentContainer = fallback;
        this.reconnect();
        return true;
      }
    }
    return false;
  }

  private pickBestContainer(): string | undefined {
    return this.pickBestContainerFromList(this.containerList, this.currentContainer);
  }

  private pickBestContainerFromList(list: string[], prefer?: string): string {
    const items = (list || []).filter(Boolean);
    if (items.length === 0) return '';
    const lower = (s: string) => (s || '').toLowerCase();
    const negatives = ['prober','probe','exporter','agent','sidecar','pause','proxy','reloader','metrics','prom','istio','linkerd','kube-rbac-proxy','configmap-reload','reloader'];
    const positivesExact = ['engine','mysql','xstore','server','main','app','dn','cn','gms','cdc'];
    const positivesContains = ['engine','mysql','xstore','server','main','app','dn-','cn-','gms','cdc'];

    if (prefer && items.some(c => c === prefer) && !negatives.some(n => lower(prefer).includes(n))) {
      return prefer;
    }
    for (const p of positivesExact) {
      const hit = items.find(c => lower(c) === p);
      if (hit) return hit;
    }
    for (const p of positivesContains) {
      const hit = items.find(c => lower(c).includes(p));
      if (hit) return hit;
    }
    const nonNeg = items.find(c => !negatives.some(n => lower(c).includes(n)));
    if (nonNeg) return nonNeg;
    return items[0];
  }

  private appendOutput(text: string): void {
    // Write raw output to keep terminal control sequences intact,
    // so backspace/cursor movement and similar operations behave correctly.
    this.term?.write(text);
    this.sessionLog += text;
  }

  private parseAnsiColors(text: string): string {
    // Remove window-title control sequence: ]0;...BEL
    let processed = text.replace(/\]0;[^\x07]*\x07/g, '');
    
    // Remove other invisible control sequences.
    processed = processed.replace(/\[\?[0-9]+[hl]/g, ''); // Mode setting
    
    // Escape HTML chars first to avoid conflicting with our injected tags.
    processed = processed
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    
    // Now handle ANSI color codes.
    const ansiColorMap: Record<string, string> = {
      '0': '#d6e4ff',    // reset/normal - light blue-white
      '1': 'bold',       // bold marker
      '30': '#000000',   // black
      '31': '#ff6b6b',   // red
      '32': '#51cf66',   // green
      '33': '#ffd43b',   // yellow
      '34': '#339af0',   // blue
      '35': '#da77f2',   // magenta
      '36': '#22b8cf',   // cyan
      '37': '#ced4da',   // white
    };
    
    // Simpler approach: replace ANSI sequences with <span> tags.
    processed = processed.replace(/\[([0-9;]+)m/g, (match, codes) => {
      const codeList = codes.split(';');
      let color = '';
      let isBold = false;
      
      for (const code of codeList) {
        if (code === '0') {
          // Reset - end current style.
          return '</span>';
        } else if (code === '1') {
          isBold = true;
        } else if (ansiColorMap[code]) {
          color = ansiColorMap[code];
        }
      }
      
      if (color || isBold) {
        const styles = [];
        if (color && color !== 'bold') styles.push(`color: ${color}`);
        if (isBold) styles.push('font-weight: bold');
        return `<span style="${styles.join('; ')}">`;
      }
      
      return ''; // Ignore unknown codes.
    });
    
    // Handle newlines.
    processed = processed.replace(/\n/g, '<br>');
    
    // Clean up empty spans and possible nesting issues.
    processed = processed.replace(/<span style=""><\/span>/g, '');
    processed = processed.replace(/<\/span><span style="([^"]*)">/g, '');
    
    return processed;
  }

  clearScreen(): void {
    try { this.term?.clear?.(); } catch {}
  }

  async copySelection(): Promise<void> {
    try {
      const sel = (this.term?.getSelection && this.term.getSelection()) || '';
      const text = sel || this.sessionLog || '';
      if (!text) return;
      await navigator.clipboard.writeText(text);
      this.term?.writeln('\r\n[复制] 已复制到剪贴板');
    } catch {
      this.term?.writeln('\r\n[复制] 失败：浏览器权限受限');
    }
  }

  async pasteFromClipboard(): Promise<void> {
    try {
      const text = await navigator.clipboard.readText();
      if (text) {
        this.ws?.send(text);
      }
    } catch {
      this.term?.writeln('\r\n[粘贴] 失败：浏览器权限受限');
    }
  }

  downloadLog(): void {
    const blob = new Blob([this.sessionLog || ''], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    a.download = `webshell-${this.data.namespace}-${this.data.pod}-${this.currentContainer||'c'}-${stamp}.log`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }
}
