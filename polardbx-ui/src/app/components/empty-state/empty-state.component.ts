import { Component, Input } from '@angular/core';
import { CommonModule } from '@angular/common';
import { NzIconModule } from 'ng-zorro-antd/icon';

@Component({
  selector: 'app-empty-state',
  standalone: true,
  imports: [CommonModule, NzIconModule],
  template: `
    <div class="empty-state">
      <i nz-icon [nzType]="icon || 'inbox'"></i>
      <p>{{ title }}</p>
      <p class="hint" *ngIf="hint">{{ hint }}</p>
    </div>
  `
})
export class EmptyStateComponent {
  @Input() icon = 'inbox';
  @Input() title = '暂无数据';
  @Input() hint = '';
}

