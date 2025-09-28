import { AfterViewInit, OnInit, Component, ElementRef, Inject, ViewChild, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatSelectModule } from '@angular/material/select';
import { MatTooltipModule } from '@angular/material/tooltip';

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
  <h2 mat-dialog-title>
    <mat-icon>computer</mat-icon>
    WebShell
  </h2>
  <div mat-dialog-content>
    <div class="toolbar">
      <mat-icon>dns</mat-icon>
      <span class="kv">ns</span><span class="vv">{{ data.namespace }}</span>
      <span class="kv">pod</span><span class="vv">{{ data.pod }}</span>
      <span class="kv">容器</span>
      <mat-select [(value)]="currentContainer" (valueChange)="onContainerChange($event)" class="container-select" [disabled]="connecting">
        <mat-option *ngFor="let c of containerList" [value]="c">{{ c }}</mat-option>
      </mat-select>
      <span class="spacer"></span>
      <button mat-icon-button matTooltip="复制" (click)="copySelection()" [disabled]="connecting"><mat-icon>content_copy</mat-icon></button>
      <button mat-icon-button matTooltip="粘贴" (click)="pasteFromClipboard()" [disabled]="connecting"><mat-icon>content_paste</mat-icon></button>
      <button mat-icon-button matTooltip="清屏 (Ctrl+L)" (click)="clearScreen()"><mat-icon>clear_all</mat-icon></button>
      <button mat-icon-button matTooltip="下载输出" (click)="downloadLog()"><mat-icon>download</mat-icon></button>
      <button mat-stroked-button (click)="reconnect()" [disabled]="connecting"><mat-icon>refresh</mat-icon> 重连</button>
    </div>
    <div #term class="terminal"></div>
    <div class="tips">
      快捷键：Ctrl+C 结束进程；Ctrl+L 清屏；如需复制/粘贴请使用上方按钮（浏览器权限限制）。
    </div>
  </div>
  <div mat-dialog-actions align="end">
    <button mat-stroked-button (click)="onClose()">关闭</button>
  </div>
  `,
  styles: [`
    .toolbar { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; font-size: 12px; }
    .kv { color: #666; margin-left: 8px; }
    .vv { font-weight: 600; margin-right: 12px; }
    .spacer { flex: 1; }
    .container-select { width: 180px; }
    .terminal { height: 60vh; min-height: 360px; width: 100%; background: #0b1020; border-radius: 6px; }
    .tips { margin-top: 8px; font-size: 12px; color: #666; }
  `],
  standalone: true,
  imports: [CommonModule, MatDialogModule, MatButtonModule, MatIconModule, MatSelectModule, MatTooltipModule]
})
export class WebShellDialogComponent implements OnInit, AfterViewInit {
  dialogRef = inject<MatDialogRef<WebShellDialogComponent>>(MatDialogRef);
  data = inject<WebShellDialogData>(MAT_DIALOG_DATA as any);
  @ViewChild('term', { static: false }) termRef!: ElementRef<HTMLDivElement>;
  private term: any;
  private ws?: WebSocket;
  private sessionLog = '';
  containerList: string[] = [];
  currentContainer = '';
  connecting = false;
  private fallbackTried = false;
  private pendingInput = '';

  ngOnInit(): void {
    // 先完成与模板双向绑定相关的初始化，避免在 AfterViewInit 里改动触发 NG0100
    this.containerList = this.data.containers || (this.data.container ? [this.data.container] : []);
    this.currentContainer = this.pickBestContainerFromList(this.containerList, this.data.container);
  }

  ngAfterViewInit(): void {
    const XTerm: any = (window as any).Terminal;
    if (XTerm) {
      this.term = new XTerm({
        fontFamily: 'Menlo, Monaco, Consolas, "Courier New", monospace',
        fontSize: 13,
        theme: { background: '#0b1020' },
        cursorBlink: true,
        convertEol: true, // 与 inline 版本保持一致
        scrollback: 2000
      });
      this.term.open(this.termRef.nativeElement);
      this.term.attachCustomKeyEventHandler?.((ev: KeyboardEvent) => {
        if ((ev.ctrlKey || ev.metaKey) && ev.key.toLowerCase() === 'l') { this.clearScreen(); return false; }
        return true;
      });
      
      // 立即绑定 onData 事件处理器
      if (this.term.onData) {
        this.term.onData((data: string) => {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            try {
              this.ws.send(data);
            } catch (e) {
              console.error('WebSocket send error:', e);
            }
          } else {
            // 如果 WebSocket 还未连接，将输入暂存
            this.pendingInput += data;
          }
        });
        (this.term as any)._dataHandlerBound = true;
      }
      
      this.termRef.nativeElement.addEventListener('click', () => this.term?.focus?.());
      // 立即聚焦终端
      this.term.focus?.();
    } else {
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
    
    // 创建简单的终端对象
    this.term = {
      writeln: (text: string) => {
        termDiv.innerHTML += text + '\n';
        termDiv.scrollTop = termDiv.scrollHeight;
      },
      write: (text: string) => {
        // 处理常见的终端控制序列
        let processed = text;
        
        // 处理退格：\b 或 \u0008
        processed = processed.replace(/\u0008/g, () => {
          // 移除最后一个字符的显示
          const current = termDiv.textContent || '';
          if (current.length > 0) {
            termDiv.textContent = current.slice(0, -1);
          }
          return '';
        });
        
        // 处理回车
        processed = processed.replace(/\r/g, '\n');
        
        // 处理 ANSI 颜色代码和控制序列
        processed = this.parseAnsiColors(processed);
        
        // 如果还有内容，添加到终端
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
        // 存储回调函数，在 WebSocket 连接建立后调用
        this.term._onDataCallback = callback;
        
        // 防重复输入的防抖机制
        let lastKeyTime = 0;
        let lastKey = '';
        
        // 简单的键盘输入处理（与内联组件一致）
        const handler = (e: KeyboardEvent) => {
          const now = Date.now();
          const keyIdentifier = e.key + (e.ctrlKey ? '+ctrl' : '') + (e.altKey ? '+alt' : '');
          
          // 防抖：相同按键在 100ms 内只处理一次
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
              case 'Backspace': payload = '\u0008'; break; // 改回 BS
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
          if (payload) { callback(payload); e.preventDefault(); e.stopPropagation(); }
        };
        termDiv.addEventListener('keydown', handler);
        // 避免过于敏感：不在 window 上绑定键盘事件，仅限终端获得焦点时生效
      }
    };
    
    // 使终端可以聚焦
    termDiv.tabIndex = 0;
    termDiv.setAttribute('role', 'textbox');
    termDiv.setAttribute('aria-multiline', 'true');
    termDiv.addEventListener('click', () => termDiv.focus());
    termDiv.focus();
  }

  onClose(): void {
    this.term?.dispose();
    try { this.ws?.close(); } catch {}
    this.dialogRef.close();
  }

  onContainerChange(_c: string): void {
    // 手动切换容器后允许再次自动回退
    this.fallbackTried = false;
    this.reconnect();
  }

  reconnect(): void {
    try { this.ws?.close(); } catch {}
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
    const backendBase = 'http://localhost:8080'; // 与 ApiService.baseUrl 对齐
    const backendHost = new URL(backendBase).host;
    const ns = encodeURIComponent(this.data.namespace);
    const pod = encodeURIComponent(this.data.pod);
    const container = encodeURIComponent(this.currentContainer || this.pickBestContainerFromList(this.containerList, this.data.container) || 'engine');
    // 默认命令：覆盖更多精简镜像
    const defaultCmd = encodeURIComponent("exec /bin/bash || exec /bin/sh || exec /bin/ash || /bin/busybox sh || /busybox sh");
    // 设置终端尺寸，改善输出格式
    const url = `${wsScheme}://${backendHost}/api/v1/pods/${ns}/${pod}/exec?container=${container}&tty=true&cmd=${defaultCmd}&k=${encodeURIComponent(kubeconfigB64)}&rows=24&cols=80`;

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
      const sendFn = (chunk: string) => {
        try {
          if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(chunk);
          }
        } catch (e) {
          console.error('WebSocket send error:', e);
        }
      };
      let bound = false;
      if (this.term && typeof this.term.onData === 'function') {
        this.term.onData((data: string) => {
          try {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
              this.ws.send(data);
            }
          } catch (e) {
            console.error('WebSocket send error:', e);
          }
        });
        bound = true;
      } else if (this.term && typeof this.term.onKey === 'function') {
        this.term.onKey((e: any) => sendFn(e?.key ?? ''));
        bound = true;
      } else if (this.term?._onDataCallback) {
        // 对于简易终端，手动调用回调设置
        const callback = (data: string) => {
          try {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
              this.ws.send(data);
            }
          } catch (e) {
            console.error('WebSocket send error:', e);
          }
        };
        this.term._onDataCallback = callback;
        bound = true;
      }
      if (!bound) {
        const el = this.termRef.nativeElement as HTMLElement;
        const handler = (e: KeyboardEvent) => {
          let out = '';
          if (e.key === 'Enter') out = '\r';
          else if (e.key === 'Backspace') out = '\u007f';
          else if (e.key === 'Tab') out = '\t';
          else if (e.key === 'ArrowUp') out = '\u001b[A';
          else if (e.key === 'ArrowDown') out = '\u001b[B';
          else if (e.key === 'ArrowRight') out = '\u001b[C';
          else if (e.key === 'ArrowLeft') out = '\u001b[D';
          else if (e.ctrlKey) { const k = e.key.toLowerCase(); const map: Record<string,string> = { c:'\u0003', d:'\u0004', z:'\u001a', a:'\u0001', e:'\u0005', l:'\u000c' }; if (map[k]) out = map[k]; }
          else if (e.key.length === 1) out = e.key;
          if (out) { e.preventDefault(); e.stopPropagation(); sendFn(out); }
        };
        el.addEventListener('keydown', handler);
        const wsInst = this.ws;
        if (wsInst) {
          wsInst.addEventListener('close', () => el.removeEventListener('keydown', handler), { once: true } as any);
        }
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
          // 拦截容器不存在错误，自动切换到更合适的容器重连一次
          if (this.handleContainerNotFound(t)) return;
          this.appendOutput(t);
        }).catch(()=>{});
        return;
      }
      if (this.handleContainerNotFound(text)) return;
      this.appendOutput(text);
    };
    this.ws.onerror = () => {
      this.term?.writeln('\r\n[错误] WebSocket 通道异常');
    };
    this.ws.onclose = () => {
      this.term?.writeln('\r\n[断开] 会话已结束');
      this.connecting = false;
      setTimeout(() => {
        if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
          this.term?.writeln('[重试] 正在重连...');
          this.connectWebSocket();
        }
      }, 1500);
    };
  }

  // 如果出现 “container not found ("xxx")” 或 “unable to upgrade connection: container not found”
  // 自动选择一个实际存在的容器重连（每次连接最多尝试一次自动修复）
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
    // 直接显示原始输出，保留所有终端控制序列
    // 这样可以正确处理退格、光标移动等
    this.term?.write(text);
    this.sessionLog += text;
  }

  private parseAnsiColors(text: string): string {
    // 移除窗口标题控制序列 ]0;...BEL
    let processed = text.replace(/\]0;[^\x07]*\x07/g, '');
    
    // 移除其他不可见控制序列
    processed = processed.replace(/\[\?[0-9]+[hl]/g, ''); // 模式设置
    
    // 先转义 HTML 字符，避免与我们的标签冲突
    processed = processed
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    
    // 现在处理 ANSI 颜色代码
    const ansiColorMap: Record<string, string> = {
      '0': '#d6e4ff',    // 重置/正常 - 浅蓝白色
      '1': 'bold',       // 粗体标记
      '30': '#000000',   // 黑色
      '31': '#ff6b6b',   // 红色
      '32': '#51cf66',   // 绿色
      '33': '#ffd43b',   // 黄色
      '34': '#339af0',   // 蓝色
      '35': '#da77f2',   // 洋红
      '36': '#22b8cf',   // 青色
      '37': '#ced4da',   // 白色
    };
    
    // 用一个更简单的方法：直接替换为 span
    processed = processed.replace(/\[([0-9;]+)m/g, (match, codes) => {
      const codeList = codes.split(';');
      let color = '';
      let isBold = false;
      
      for (const code of codeList) {
        if (code === '0') {
          // 重置 - 结束当前样式
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
      
      return ''; // 忽略不识别的代码
    });
    
    // 处理换行
    processed = processed.replace(/\n/g, '<br>');
    
    // 清理可能的空 span 和嵌套问题
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

