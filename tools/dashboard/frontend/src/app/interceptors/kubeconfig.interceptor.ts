import { HttpInterceptorFn } from '@angular/common/http';
import { appendKubeconfigHeader } from '../utils/http-headers';

const API_PREFIX = '/api/v1/';
const KUBECONFIG_HEADER = 'X-Kubeconfig-B64';

export const kubeconfigHeaderInterceptor: HttpInterceptorFn = (req, next) => {
  if (!req.url.includes(API_PREFIX)) {
    return next(req);
  }

  if (req.headers.has(KUBECONFIG_HEADER)) {
    return next(req);
  }

  const headers = appendKubeconfigHeader(req.headers);
  if (headers === req.headers) {
    return next(req);
  }

  return next(req.clone({ headers }));
};
