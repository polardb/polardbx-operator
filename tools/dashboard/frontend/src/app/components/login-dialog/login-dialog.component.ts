import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ReactiveFormsModule, FormBuilder, Validators } from '@angular/forms';
import { MatDialogRef, MatDialogModule } from '@angular/material/dialog';
import { MatCardModule } from '@angular/material/card';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { AuthService } from '../../services/auth.service';

@Component({
  selector: 'app-login-dialog',
  standalone: true,
  imports: [CommonModule, ReactiveFormsModule, MatDialogModule, MatCardModule, MatFormFieldModule, MatInputModule, MatButtonModule, MatIconModule],
  template: `
    <mat-card>
      <mat-card-title>登录</mat-card-title>
      <form [formGroup]="form" (ngSubmit)="submit()" class="grid">
        <mat-form-field appearance="outline">
          <mat-label>用户名</mat-label>
          <input matInput formControlName="username" autofocus>
        </mat-form-field>
        <mat-form-field appearance="outline">
          <mat-label>密码</mat-label>
          <input matInput type="password" formControlName="password">
        </mat-form-field>
        <div class="actions">
          <button mat-stroked-button type="button" (click)="close()">取消</button>
          <button mat-raised-button color="primary" type="submit" [disabled]="form.invalid">登录</button>
        </div>
      </form>
    </mat-card>
  `,
  styles: [`
    .grid { display: grid; gap: 12px; min-width: 320px; }
    .actions { display: flex; gap: 8px; justify-content: flex-end; }
  `]
})
export class LoginDialogComponent {
  private fb = inject(FormBuilder);
  private auth = inject(AuthService);
  private ref = inject(MatDialogRef<LoginDialogComponent>);

  form = this.fb.group({ username: ['', Validators.required], password: ['', Validators.required] });

  async submit() {
    const { username, password } = this.form.value as any;
    await this.auth.login(username, password);
    this.ref.close(true);
  }
  close() { this.ref.close(false); }
}

