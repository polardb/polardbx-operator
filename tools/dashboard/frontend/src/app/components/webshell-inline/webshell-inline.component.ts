import { AfterViewInit, OnInit, OnChanges, OnDestroy, SimpleChanges, Component, ElementRef, ViewChild, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';

// Import local XTerm packages directly.
import { Terminal } from 'xterm';
import { FitAddon } from '@xterm/addon-fit';
import { SerializeAddon } from '@xterm/addon-serialize';

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

@Component({
  selector: 'app-webshell-inline',
  standalone: true,
  imports: [CommonModule, MatButtonModule, MatIconModule, MatSelectModule, MatTooltipModule],
  template: `
  <div class="shell-card">
    <div class="shell-toolbar">
      <div class="left">
        <mat-icon class="title-icon">terminal</mat-icon>
        <div class="title">Terminal</div>
        <div class="chips">
          <span class="chip" matTooltip="命名空间">ns: {{ namespace }}</span>
          <span class="chip pod" matTooltip="{{ pod }}">pod: {{ pod }}</span>
        </div>
        <div class="selector">
          <span class="label">容器</span>
          <mat-select [(value)]="currentContainer" (valueChange)="onContainerChange($event)" class="container-select" [disabled]="connecting" [disableOptionCentering]="true" panelClass="force-above-panel">
            <mat-option *ngFor="let c of containerList" [value]="c">{{ c }}</mat-option>
          </mat-select>
        </div>
      </div>
      <div class="right">
        <button mat-icon-button matTooltip="复制" (click)="copySelection()" [disabled]="connecting"><mat-icon>content_copy</mat-icon></button>
        <button mat-icon-button matTooltip="粘贴" (click)="pasteFromClipboard()" [disabled]="connecting"><mat-icon>content_paste</mat-icon></button>
        <button mat-icon-button matTooltip="清屏 (Ctrl+L)" (click)="clearScreen()"><mat-icon>clear_all</mat-icon></button>
        <button mat-icon-button matTooltip="下载输出" (click)="downloadLog()"><mat-icon>download</mat-icon></button>
        <button mat-icon-button matTooltip="重连" (click)="reconnect()"><mat-icon>refresh</mat-icon></button>
        <span class="status-dot" [ngClass]="getStatusClass()" matTooltip="{{ getStatusText() }}"></span>
      </div>
    </div>
    <div #term class="terminal"></div>
    <div class="tips">快捷键：Ctrl+C 结束进程；Ctrl+L 清屏；若无响应请点击右上角重连。</div>
  </div>
  `,
  styles: [`
    .shell-card { border: 1px solid #e6e8ef; border-radius: 10px; overflow: hidden; background: #fff; box-shadow: 0 2px 8px rgba(16,24,40,.06); }
    .shell-toolbar { 
      display:flex; 
      align-items:center; 
      justify-content: space-between; 
      padding: 8px 12px; 
      background: linear-gradient(180deg,#f8fafc, #f3f6fb); 
      border-bottom: 1px solid #e6e8ef; 
      position: relative; 
      z-index: 20; 
    }
    .left { display:flex; align-items:center; gap: 10px; min-width: 0; }
    .title-icon { color:#334155; }
    .title { font-weight: 600; color:#1f2937; }
    .chips { display:flex; gap:6px; align-items:center; }
    .chip { font-size: 11px; background:#eef2f7; color:#334155; padding:2px 8px; border-radius: 999px; white-space: nowrap; }
    .chip.pod { max-width: 260px; overflow: hidden; text-overflow: ellipsis; }
    .selector { display:flex; align-items:center; gap:6px; margin-left: 8px; position: relative; z-index: 21; }
    .selector .label { font-size: 12px; color:#64748b; }
    .container-select { width: 160px; height: 32px; }
    .right { display:flex; align-items:center; gap: 6px; }
    .status-dot { width:10px; height:10px; border-radius:50%; display:inline-block; margin-left:4px; border:1px solid rgba(0,0,0,.1); }
    .status-ok { background:#10b981; box-shadow: 0 0 0 2px rgba(16,185,129,.15); }
    .status-connecting { background:#f59e0b; box-shadow: 0 0 0 2px rgba(245,158,11,.15); }
    .status-down { background:#ef4444; box-shadow: 0 0 0 2px rgba(239,68,68,.15); }
    .terminal { height: 60vh; min-height: 360px; width: 100%; background: #0b1020; position: relative; z-index: 1; }
    .tips { padding:8px 12px; font-size: 12px; color: #667085; background:#fafbfc; border-top: 1px solid #e6e8ef; }

    /* Force the dropdown panel to open above to avoid being covered by the dark terminal background. */
    ::ng-deep .force-above-panel {
      z-index: 100000 !important;
      position: fixed !important;
      pointer-events: auto !important;
      background: white !important;
      box-shadow: 0 8px 24px rgba(0,0,0,0.3) !important;
    }
    
    /* Ensure the WebShell inline dropdown is rendered on top. */
    ::ng-deep .cdk-overlay-container {
      z-index: 100000 !important;
      pointer-events: none !important;
    }
    ::ng-deep .cdk-overlay-pane {
      z-index: 100000 !important;
      position: fixed !important;
      pointer-events: auto !important;
    }
    ::ng-deep .mat-mdc-select-panel {
      z-index: 100000 !important;
      position: fixed !important;
      pointer-events: auto !important;
      background: white !important;
      box-shadow: 0 8px 24px rgba(0,0,0,0.3) !important;
      border: 1px solid #ddd !important;
    }
    ::ng-deep .mat-mdc-option {
      z-index: 100000 !important;
      pointer-events: auto !important;
    }
    ::ng-deep .container-select .cdk-overlay-pane {
      z-index: 100000 !important;
      pointer-events: auto !important;
    }
    
    /* Ensure the dark terminal background does not cover dropdowns. */
    .terminal {
      position: relative;
      z-index: 1;
    }
    
    /* Ensure toolbar dropdown renders correctly. */
    .shell-toolbar .container-select {
      position: relative;
      z-index: 22;
    }
  `]
})
export class WebShellInlineComponent implements OnInit, OnChanges, AfterViewInit, OnDestroy {
  private hasSeenServerOutput = false;
  @Input() namespace = 'default';
  @Input() pod = '';
  @Input() containers: string[] = [];
  @Input() container: string | undefined;

  @ViewChild('term', { static: false }) termRef!: ElementRef<HTMLDivElement>;
  private term: any;
  private ws?: WebSocket;
  private sessionLog = '';
  containerList: string[] = [];
  currentContainer = '';
  connecting = false;
  private fallbackTried = false;
  private pendingInput = '';
  private localEcho = false; // Disable local echo; let the server handle all echo/output.
  private outputBuffer = ''; // Output buffer for batch processing.
  private fitAddon: any; // FitAddon instance
  private serializeAddon: any; // SerializeAddon instance
  private resizeHandler?: () => void; // Window resize handler

  private echoLocally(data: string): void {
    try {
      if (!this.localEcho) return;
      if (!this.term?.write) return;
      // Echo only printable characters and common control keys (Enter, Backspace, Tab).
      let out = '';
      for (let i = 0; i < data.length; i++) {
        const ch = data[i];
        const code = ch.charCodeAt(0);
        if (ch === '\\r') { out += '\\r\\n'; continue; }
        if (ch === '\\t') { out += '\\t'; continue; }
        if (code === 0x08 || code === 0x7f) { // Backspace/Delete -> delegate to backend and let the terminal handle it.
          out += '\\b';
          continue;
        }
        // Filter sequences starting with ESC (arrow keys, etc.).
        if (code === 0x1b) {
          // Skip the sequence (simple handling: do not echo locally).
          continue;
        }
        if (code >= 32 && code <= 126) {
          out += ch;
        }
      }
      if (out) this.term.write(out);
    } catch {}
  }


  ngOnInit(): void {
    this.containerList = this.containers || (this.container ? [this.container] : []);
    this.currentContainer = this.pickBestContainerFromList(this.containerList, this.container);
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['containers'] || changes['container']) {
      this.containerList = this.containers || (this.container ? [this.container] : []);
      const next = this.pickBestContainerFromList(this.containerList, this.container || this.currentContainer);
      if (next && next !== this.currentContainer) {
        this.currentContainer = next;
        // If the terminal is initialized, auto-reconnect after switching containers.
        if (this.term) {
          // If already connected, reconnect; otherwise connect directly.
          if (this.ws) {
            this.reconnect();
          } else {
            this.connectWebSocketFast();
          }
        }
      }
    }
  }

  ngAfterViewInit(): void {
    // Prefer the standard xterm implementation; fall back to a simple terminal on failure.
    try {
      this.initXtermStandard();
    } catch (e) {
      console.warn('xterm 初始化失败，回退到简单终端:', e);
      this.initFastTerminal();
    }
    if (this.currentContainer) this.connectWebSocketFast();
  }

  ngAfterViewInitOld(): void {
    // Use locally imported XTerm directly; do not rely on a CDN.
    const init = async () => {
      try {
        console.log('Initializing local XTerm...');
        
        // Use the locally imported Terminal directly.
        this.term = new Terminal({
          fontFamily: 'Menlo, Monaco, Consolas, "Courier New", monospace',
          fontSize: 13,
          lineHeight: 1.2,
          theme: { 
            background: '#0b1020',
            foreground: '#d6e4ff',
            cursor: '#ffffff',
            cursorAccent: '#ffffff'
          },
          cursorBlink: true,
          convertEol: true,
          scrollback: 10000,
          fastScrollModifier: 'alt',
          rightClickSelectsWord: true,
          allowProposedApi: true
        });
        
        // Load FitAddon to support auto-resizing.
        try {
          this.fitAddon = new FitAddon();
          this.term.loadAddon(this.fitAddon);
          
          // Auto-fit terminal when the window resizes.
          this.resizeHandler = () => {
            if (this.fitAddon && this.term) {
              this.fitAddon.fit();
            }
          };
          window.addEventListener('resize', this.resizeHandler);
          
          // Fit once after initialization.
          setTimeout(() => {
            this.fitAddon?.fit();
          }, 100);
        } catch (e) {
          console.warn('FitAddon initialization failed:', e);
        }
        
        // Load SerializeAddon to support log download.
        try {
          this.serializeAddon = new SerializeAddon();
          this.term.loadAddon(this.serializeAddon);
        } catch (e) {
          console.warn('SerializeAddon initialization failed:', e);
        }
        
        this.term.open(this.termRef.nativeElement);
        
        // Custom keyboard shortcut handling.
        this.term.attachCustomKeyEventHandler?.((ev: KeyboardEvent) => {
          if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === 'l') { 
            this.clearScreen(); 
            return false; 
          }
          return true;
        });
        
        // Bind input handling (delay until WebSocket is connected to avoid duplicate bindings).
        // Do not bind onData here; bind it after WebSocket connection is established.
        console.log('XTerm terminal ready, waiting for WebSocket connection to bind events');
        
        // Focus on click.
        this.termRef.nativeElement.addEventListener('click', () => this.term?.focus?.());
        this.term.focus?.();
        
        console.log('Local XTerm initialized successfully');
        
      } catch (e) {
        console.error('Failed to initialize local XTerm:', e);
        console.warn('Falling back to simple terminal');
        this.initSimpleTerminal();
      }
      
      // Delay connection and try high-performance kubectl proxy mode first.
      setTimeout(() => {
        // Check if kubectl proxy is available.
        fetch('http://localhost:8005/health')
          .then(response => response.json())
          .then(data => {
            if (data.status === 'ok') {
              console.log('Using high-performance kubectl proxy mode');
              this.connectToKubectlProxy();
            } else {
              console.log('kubectl proxy not available, using standard mode');
              this.connectWebSocket();
            }
          })
          .catch(() => {
            console.log('kubectl proxy not available, using standard mode');
            this.connectWebSocket();
          });
      }, 100);
    };
    void init();
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
    
    // Create a high-performance simple terminal wrapper.
    this.term = {
      writeln: (text: string) => {
        termDiv.textContent += text + '\n';
        termDiv.scrollTop = termDiv.scrollHeight;
      },
      write: (text: string) => {
        // Write directly without heavy processing to minimize latency.
        termDiv.textContent += text;
        termDiv.scrollTop = termDiv.scrollHeight;
      },
      clear: () => {
        termDiv.textContent = '';
      },
      focus: () => {
        termDiv.focus();
      },
      onData: (callback: (data: string) => void) => {
        // Store callback.
        (this.term as any)._onDataCallback = callback;
        
        // Simplified keyboard input handling: send directly.
        const handler = (e: KeyboardEvent) => {
          e.preventDefault();
          e.stopPropagation();
          
          let payload = '';
          
          // Control-key combinations
          if (e.ctrlKey && !e.altKey && !e.metaKey) {
            const ctrlMap: Record<string, string> = {
              'c': '\u0003', 'd': '\u0004', 'z': '\u001a',
              'a': '\u0001', 'e': '\u0005', 'l': '\u000c'
            };
            payload = ctrlMap[e.key.toLowerCase()] || '';
          } else {
            // Regular key handling
            switch (e.key) {
              case 'Enter': payload = '\r'; break;
              case 'Backspace': payload = '\u007f'; break;
              case 'Tab': payload = '\t'; break;
              case 'ArrowUp': payload = '\u001b[A'; break;
              case 'ArrowDown': payload = '\u001b[B'; break;
              case 'ArrowRight': payload = '\u001b[C'; break;
              case 'ArrowLeft': payload = '\u001b[D'; break;
              default:
                if (e.key.length === 1 && !e.metaKey) payload = e.key;
            }
          }
          
          if (payload) {
            callback(payload);
          }
        };
        
        termDiv.addEventListener('keydown', handler);
      }
    };
    
    // Make the terminal focusable.
    termDiv.tabIndex = 0;
    termDiv.setAttribute('role', 'textbox');
    termDiv.addEventListener('click', () => termDiv.focus());
    termDiv.focus();
    
    // Attach keyboard listener immediately.
    if (this.term && 'onData' in this.term) {
      this.term.onData((data: string) => {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          try {
            this.ws.send(data);
          } catch (e) {
            console.error('WebSocket send error:', e);
          }
        } else {
          this.pendingInput += data;
        }
      });
      (this.term as any)._dataHandlerBound = true;
    }
  }

  onContainerChange(_c: string): void {
    this.fallbackTried = false;
    this.reconnect();
  }

  reconnect(): void {
    try { 
      this.ws?.close(); 
    } catch (error: unknown) {
      console.warn('Failed to close WebSocket:', error);
    }
    // Reset binding flags to allow rebinding.
    if (this.term) {
      (this.term as any)._dataHandlerBound = false;
    }
    console.log('Reconnecting WebSocket, reset event binding flag');
    this.connectWebSocket(0); // Reset retry counter.
  }

  private connectToKubectlProxy() {
    const wsUrl = `ws://localhost:8004?namespace=${this.namespace}&pod=${this.pod}&container=${this.currentContainer}`;
    console.log('Connecting to kubectl proxy:', wsUrl);
    
    this.ws = new WebSocket(wsUrl);
    this.connecting = true;
    
    this.ws.onopen = () => {
      this.connecting = false;
      this.term?.writeln(`[高性能模式] 已连接到 ${this.currentContainer || '-'}`);
      this.term?.focus();
      this.sessionLog = '';
      
      // Bind XTerm event handlers - high-performance mode, no local echo.
      if (this.term && this.term.onData && !(this.term as any)._kubectlProxyBound) {
        this.term.onData((data: string) => {
          // High-performance mode: send data directly without local echo.
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(data);
          }
        });
        (this.term as any)._kubectlProxyBound = true;
      }
    };
    
    this.ws.onmessage = (event) => {
      if (typeof event.data === 'string') {
        this.term?.write(event.data);
        this.sessionLog += event.data;
      }
    };
    
    this.ws.onerror = (error) => {
      console.error('kubectl proxy connection error:', error);
      this.term?.writeln('\r\n[错误] kubectl proxy连接失败，切换到普通模式...');
      this.connectWebSocket(0); // Fall back to standard mode.
    };
    
    this.ws.onclose = () => {
      this.term?.writeln('\r\n[断开] kubectl proxy连接已关闭');
      this.connecting = false;
    };
  }

  private connectWebSocket(retryCount: number = 0): void {
    // If not ready (no container), wait until inputs are populated.
    if (!this.currentContainer) {
      const pick = this.pickBestContainerFromList(this.containerList, this.container);
      this.currentContainer = pick || '';
      if (!this.currentContainer) {
        this.term?.writeln('[等待] 尚未获取到容器列表，稍后重试...');
        return;
      }
    }
    const kubeconfig = localStorage.getItem('kubeconfig') || '';
    if (!kubeconfig) {
      this.term?.writeln('未找到 kubeconfig，请先连接 Kubernetes 集群。');
      return;
    }
    const kubeconfigB64 = btoa(unescape(encodeURIComponent(kubeconfig)));
    const wsScheme = (window.location?.protocol || 'http:') === 'https:' ? 'wss' : 'ws';
    const backendBase = 'http://localhost:8080';
    const backendHost = new URL(backendBase).host;
    const ns = encodeURIComponent(this.namespace);
    const pod = encodeURIComponent(this.pod);
    const container = encodeURIComponent(this.currentContainer || this.pickBestContainerFromList(this.containerList, this.container) || 'engine');
    const defaultCmd = encodeURIComponent('exec /bin/bash || exec /bin/sh || exec /bin/ash || /bin/busybox sh || /busybox sh');
    const url = `${wsScheme}://${backendHost}/api/v1/pods/${ns}/${pod}/exec?container=${container}&tty=true&cmd=${defaultCmd}&k=${encodeURIComponent(kubeconfigB64)}`;

    this.connecting = true;
    
    try { 
      this.ws = new WebSocket(url); 
    } catch (e) { 
      this.term?.writeln(`WebSocket 连接失败: ${(e as any)?.message || e}`); 
      this.connecting = false; 
      this.scheduleReconnect(retryCount);
      return; 
    }

    this.ws.binaryType = 'arraybuffer';
    this.ws.onopen = () => {
      this.connecting = false;
      this.fallbackTried = false;
      this.term?.writeln(`[已连接] 容器=${this.currentContainer || '-'}，按 Ctrl+C 结束进程`);
      this.term?.focus();
      this.sessionLog = '';
      
      // Ensure XTerm onData handler is correctly bound.
      if (this.term && this.term.onData && !(this.term as any)._dataHandlerBound) {
        console.log('Binding XTerm onData handler...');
        this.term.onData((data: string) => {
          console.log('XTerm data received:', data.charCodeAt(0), data);
          
          // No local echo; let the server handle all echo/output.
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            try {
              this.ws.send(data);
            } catch (e) {
              console.error('WebSocket send error:', e);
            }
          } else {
            this.pendingInput += data;
          }
        });
        (this.term as any)._dataHandlerBound = true;
      }
      
      // Bind keyboard handler for the simple terminal (if applicable).
      if (this.term && '_onDataCallback' in this.term && !(this.term as any)._dataHandlerBound) {
        console.log('Binding simple terminal handler...');
        (this.term as any)._onDataCallback = (data: string) => {
          console.log('Simple terminal data received:', data.charCodeAt(0), data);
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            try {
              this.ws.send(data);
              console.log('Data sent to WebSocket');
            } catch (e) {
              console.error('WebSocket send error:', e);
            }
          } else {
            this.pendingInput += data;

          }
        };
        (this.term as any)._dataHandlerBound = true;

      }
      
      // Flush any pending input
      if (this.pendingInput) {
        this.ws?.send(this.pendingInput);
        this.pendingInput = '';
      }
      

      

    };
    this.ws.onmessage = (ev) => {
      const data = ev.data;
      let text = '';
      if (typeof data === 'string') { 
        text = data; 
      }
      else if (data instanceof ArrayBuffer) { 
        text = new TextDecoder().decode(new Uint8Array(data)); 
      }
      else if (data && typeof (data as any).arrayBuffer === 'function') {
        (data as Blob).arrayBuffer().then(buf => {
          const t = new TextDecoder().decode(new Uint8Array(buf));
          if (this.handleContainerNotFound(t)) return;
          this.appendOutput(t);
        }).catch(()=>{});
        return;
      }
      
      if (this.handleContainerNotFound(text)) return;
      this.appendOutput(text);
    };
    
    if (this.ws) {
      this.ws.onerror = (error) => { 
        console.error('WebSocket error:', error);
        this.term?.writeln('\r\n[错误] WebSocket 通道异常，尝试重连...'); 
        this.scheduleReconnect(retryCount);
      };
      this.ws.onclose = (event) => {
        console.log('WebSocket closed:', event.code, event.reason);
        this.term?.writeln(`\r\n[断开] 会话已结束 (${event.code}: ${event.reason || 'no reason'})`);
        this.connecting = false;
        // Auto-reconnect on abnormal close.
        if (event.code !== 1000 && retryCount < 3) {
          this.scheduleReconnect(retryCount);
        }
      };
    }
  }

  // Smart reconnect mechanism
  private scheduleReconnect(retryCount: number): void {
    if (retryCount >= 3) {
      this.term?.writeln('\r\n[错误] 重连失败次数过多，请手动重连');
      return;
    }
    
    const delay = Math.min(1000 * Math.pow(2, retryCount), 5000); // Exponential backoff, max 5s.
    this.term?.writeln(`\r\n[重连] ${delay / 1000}秒后自动重连 (${retryCount + 1}/3)...`);
    
    setTimeout(() => {
      if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
        this.connectWebSocket(retryCount + 1);
      }
    }, delay);
  }

  getStatusClass(): string {
    if (this.connecting) return 'status-connecting';
    return this.ws && this.ws.readyState === WebSocket.OPEN ? 'status-ok' : 'status-down';
  }
  getStatusText(): string {
    if (this.connecting) return '正在连接';
    return this.ws && this.ws.readyState === WebSocket.OPEN ? '已连接' : '未连接';
  }

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
    if (prefer && items.some(c => c === prefer) && !negatives.some(n => lower(prefer).includes(n))) return prefer;
    for (const p of positivesExact) { const hit = items.find(c => lower(c) === p); if (hit) return hit; }
    for (const p of positivesContains) { const hit = items.find(c => lower(c).includes(p)); if (hit) return hit; }
    const nonNeg = items.find(c => !negatives.some(n => lower(c).includes(n))); if (nonNeg) return nonNeg;
    return items[0];
  }

  private appendOutput(text: string): void {
    if (!this.term) return;
    
    // Write directly to the terminal to minimize latency.
    try {
      this.term.write(text);
      this.sessionLog += text;
    } catch (e) {
      console.error('Error writing to terminal:', e);
    }
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
    
    // Handle newlines - only process standalone \n to avoid double-handling \r\n.
    processed = processed.replace(/(?<!\r)\n/g, '<br>');
    
    // Clean up empty spans and possible nesting issues.
    processed = processed.replace(/<span style=""><\/span>/g, '');
    processed = processed.replace(/<\/span><span style="([^"]*)">/g, '');
    
    return processed;
  }

  clearScreen(): void { 
    try { 
      this.term?.clear?.(); 
    } catch (error: unknown) {
      console.warn('Failed to clear terminal:', error);
    } 
  }

  async copySelection(): Promise<void> {
    try { 
      const sel = (this.term?.getSelection && this.term.getSelection()) || ''; 
      const text = sel || this.sessionLog || ''; 
      if (!text) return; 
      await navigator.clipboard.writeText(text); 
      this.term?.writeln('\r\n[复制] 已复制到剪贴板'); 
    } catch (error: unknown) { 
      console.warn('Copy failed:', error);
      this.term?.writeln('\r\n[复制] 失败：浏览器权限受限'); 
    }
  }

  async pasteFromClipboard(): Promise<void> {
    try { 
      const text = await navigator.clipboard.readText(); 
      if (text) { 
        this.ws?.send(text); 
      } 
    } catch (error: unknown) { 
      console.warn('Paste failed:', error);
      this.term?.writeln('\r\n[粘贴] 失败：浏览器权限受限'); 
    }
  }

  downloadLog(): void {
    // Prefer SerializeAddon to capture full terminal content (including colors and formatting).
    const terminalContent = this.serializeAddon ? this.serializeAddon.serialize() : this.sessionLog;
    const blob = new Blob([terminalContent || ''], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    a.download = `webshell-${this.namespace}-${this.pod}-${this.currentContainer||'c'}-${stamp}.log`;
    document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
  }

  ngOnDestroy(): void {
    // Clean up WebSocket connection.
    try {
      this.ws?.close();
    } catch (error) {
      console.warn('Error closing WebSocket:', error);
    }

    // Clean up event listeners.
    if (this.resizeHandler) {
      window.removeEventListener('resize', this.resizeHandler);
    }

    // Clean up terminal addons.
    try {
      this.serializeAddon?.dispose();
    } catch (error) {
      console.warn('Error disposing SerializeAddon:', error);
    }

    try {
      this.fitAddon?.dispose();
    } catch (error) {
      console.warn('Error disposing FitAddon:', error);
    }

    // Clean up terminal instance.
    try {
      this.term?.dispose();
    } catch (error) {
      console.warn('Error disposing terminal:', error);
    }
  }

  // Minimal fast terminal initialization
  private initFastTerminal(): void {
    const termDiv = this.termRef.nativeElement;
    termDiv.style.fontFamily = 'Monaco, Consolas, monospace';
    termDiv.style.fontSize = '14px';
    termDiv.style.color = '#ffffff';
    termDiv.style.backgroundColor = '#000000';
    termDiv.style.padding = '8px';
    termDiv.style.overflow = 'auto';
    termDiv.style.whiteSpace = 'pre-wrap';
    termDiv.style.height = '100%';
    termDiv.style.outline = 'none';
    termDiv.innerHTML = '[连接中...]';
    termDiv.tabIndex = 0;
    
    // Minimal terminal wrapper
    this.term = {
      write: (text: string) => {
        termDiv.textContent += text;
        termDiv.scrollTop = termDiv.scrollHeight;
      },
      writeln: (text: string) => {
        termDiv.textContent += text + '\n';
        termDiv.scrollTop = termDiv.scrollHeight;
      },
      clear: () => {
        termDiv.textContent = '';
      },
      focus: () => {
        termDiv.focus();
      },
      dispose: () => {
        // Provide a no-op implementation to avoid errors during disposal.
        try { termDiv.textContent = ''; } catch {}
      }
    };

    console.log('✅ 极简终端初始化完成');
    
    // Focus immediately and attach click handler.
    termDiv.focus();
    termDiv.addEventListener('click', () => {
      termDiv.focus();
      console.log('🎯 终端获得焦点');
    });
    
    this.setupKeyboardHandler();
  }

  // Standard xterm implementation
  private initXtermStandard(): void {
    const termDiv = this.termRef.nativeElement;
    this.term = new Terminal({
      fontFamily: 'Menlo, Monaco, Consolas, "Courier New", monospace',
      fontSize: 13,
      lineHeight: 1.2,
      convertEol: true,
      scrollback: 10000,
      cursorBlink: true,
      allowProposedApi: true
    });
    this.fitAddon = new FitAddon();
    this.serializeAddon = new SerializeAddon();
    this.term.loadAddon(this.fitAddon);
    this.term.loadAddon(this.serializeAddon);
    this.term.open(termDiv);
    setTimeout(() => this.fitAddon.fit(), 0);

    // Keyboard input -> stdin JSON
    this.term.onData((data: string) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
      const payload = this.b64Encode(data);
      this.ws.send(JSON.stringify({ op: 'stdin', data: payload }));
    });

    // Window resize -> rows/cols
    this.resizeHandler = () => {
      try {
        this.fitAddon?.fit();
        const dims = (this.term as any)?.rows && (this.term as any)?.cols ? { rows: (this.term as any).rows, cols: (this.term as any).cols } : { rows: 24, cols: 80 };
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
          this.ws.send(JSON.stringify({ op: 'resize', rows: dims.rows, cols: dims.cols }));
        }
      } catch {}
    };
    window.addEventListener('resize', this.resizeHandler);
  }

  private b64Encode(text: string): string { return btoa(unescape(encodeURIComponent(text))); }
  private b64Decode(b64: string): string { return decodeURIComponent(escape(atob(b64))); }

  // Dedicated keyboard handler setup
  private setupKeyboardHandler(): void {
    const termDiv = this.termRef.nativeElement;
    
    // Minimal keyboard handling - improved version
    termDiv.addEventListener('keydown', (e: KeyboardEvent) => {
      console.log('⌨️ 按键事件:', e.key, 'WebSocket状态:', this.ws?.readyState);
      
      // Prevent default behavior even when WebSocket is not connected.
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        console.warn('⚠️ WebSocket 未连接，按键被忽略');
        e.preventDefault();
        return;
      }
      
      let data = '';
      if (e.key === 'Enter') {
        data = '\r';
      } else if (e.key === 'Backspace') {
        data = '\x7f';
      } else if (e.key.length === 1) {
        data = e.key;
      } else if (e.key === 'ArrowUp') {
        data = '\x1b[A';
      } else if (e.key === 'ArrowDown') {
        data = '\x1b[B';
      } else if (e.key === 'ArrowRight') {
        data = '\x1b[C';
      } else if (e.key === 'ArrowLeft') {
        data = '\x1b[D';
      }
      
      if (data) {
        e.preventDefault();
        // onData is already bound via standard xterm; keep this only for the simple-terminal fallback.
        if (!this.term?.onData) {
          try { this.ws.send(JSON.stringify({ op: 'stdin', data: this.b64Encode(data) })); } catch (err) { console.error('❌ 发送失败:', err); }
        }
      }
    });
    
    // Add keypress/input as alternative events.
    termDiv.addEventListener('keypress', (e: KeyboardEvent) => {
      console.log('⌨️ keypress事件:', e.key, e.charCode);
    });
    
    console.log('✅ 键盘事件监听器已设置');
  }

  // Minimal WebSocket connection
  private connectWebSocketFast(): void {
    console.log('🔗 开始建立 WebSocket 连接...');
    
    if (!this.currentContainer) {
      const pick = this.pickBestContainerFromList(this.containerList, this.container);
      this.currentContainer = pick || '';
      if (!this.currentContainer) {
        this.term?.writeln('[等待] 尚未获取到容器列表，稍后重试...');
        console.warn('⏳ 容器列表未就绪，2秒后重试连接');
        setTimeout(() => this.connectWebSocketFast(), 2000);
        return;
      }
    }

    const kubeconfig = localStorage.getItem('kubeconfig') || '';
    if (!kubeconfig) {
      this.term?.writeln('[错误] 未找到 kubeconfig');
      console.error('❌ 未找到 kubeconfig');
      return;
    }

    const kubeconfigB64 = btoa(unescape(encodeURIComponent(kubeconfig)));
    const wsScheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const backendHost = 'localhost:8080';
    const ns = encodeURIComponent(this.namespace);
    const pod = encodeURIComponent(this.pod);
    const container = encodeURIComponent(this.currentContainer);
    const url = `${wsScheme}://${backendHost}/api/v1/pods/${ns}/${pod}/exec?container=${container}&k=${encodeURIComponent(kubeconfigB64)}`;

    console.log('🌐 WebSocket URL:', url);
    console.log('📦 容器:', this.currentContainer);

    // Close existing connection.
    if (this.ws) {
      this.ws.close();
    }

    this.connecting = true;
    this.hasSeenServerOutput = false;
    this.ws = new WebSocket(url);
    this.ws.binaryType = 'arraybuffer';

    this.ws.onopen = () => {
      console.log('✅ WebSocket 连接成功');
      // Sync size immediately after connection.
      try {
        const rows = (this.term as any)?.rows || 24;
        const cols = (this.term as any)?.cols || 80;
        this.ws?.send(JSON.stringify({ op: 'resize', rows, cols }));
      } catch {}
      this.term?.write('\r\n[已连接] 容器=' + this.currentContainer + '，按 Ctrl+C 结束进程\r\n');
      this.connecting = false;
      
      // Re-focus after successful connection.
      setTimeout(() => {
        const termDiv = this.termRef.nativeElement;
        termDiv.focus();
        console.log('🎯 WebSocket 连接后重新获得焦点');
      }, 100);
    };

    this.ws.onmessage = (ev) => {
      try {
        if (typeof ev.data === 'string') {
          const msg = JSON.parse(ev.data);
          this.processWsMsg(msg);
        } else if (ev.data instanceof ArrayBuffer) {
          const text = new TextDecoder().decode(new Uint8Array(ev.data));
          const msg = JSON.parse(text);
          this.processWsMsg(msg);
        }
      } catch {
        // Ignore unparseable messages.
      }
    };

    this.ws.onerror = (error) => {
      console.error('❌ WebSocket 错误:', error);
      this.term?.writeln('\r\n[错误] 连接失败');
      this.connecting = false;
    };

    this.ws.onclose = (event) => {
      console.log('🔚 WebSocket 关闭:', event.code, event.reason);
      this.term?.writeln('\r\n[连接已关闭]');
      this.connecting = false;
    };
  }

  // Handle backend JSON messages
  private processWsMsg(msg: any): void {
    if (!msg || typeof msg !== 'object') return;
    const op = (msg.op || '').toString().toLowerCase();
    if (op === 'stdout' || op === 'stderr') {
      const data = typeof msg.data === 'string' ? msg.data : '';
      try {
        const text = this.b64Decode(data);
        this.term?.write?.(text);
      } catch {}
      return;
    }
    if (op === 'error') {
      const m = msg.message || 'unknown error';
      this.term?.writeln?.(`\r\n[错误] ${m}\r\n`);
      return;
    }
    // Ignore other message types.
  }
}
