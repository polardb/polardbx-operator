import { Injectable, inject } from '@angular/core';
import { CanActivate, Router, ActivatedRouteSnapshot, RouterStateSnapshot } from '@angular/router';
import { AuthService } from '../services/auth.service';

@Injectable({
  providedIn: 'root'
})
export class AuthGuard implements CanActivate {
  private readonly authService = inject(AuthService);
  private readonly router = inject(Router);

  canActivate(
    route: ActivatedRouteSnapshot,
    state: RouterStateSnapshot): boolean {
    const isAuthenticated = this.authService.isAuthenticated();
    console.log('AuthGuard检查认证状态:', isAuthenticated);
    console.log('当前路由:', state.url);
    console.log('kubeconfig存在:', !!this.authService.getKubeconfig());
    
    if (isAuthenticated) {
      console.log('认证通过，允许访问');
      return true;
    } else {
      console.log('认证失败，跳转到连接页面');
      this.router.navigate(['/connect']);
      return false;
    }
  }
} 