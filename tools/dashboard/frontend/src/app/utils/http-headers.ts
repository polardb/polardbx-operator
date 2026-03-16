import { HttpHeaders } from '@angular/common/http';

const KUBECONFIG_KEY = 'kubeconfig';

export function appendKubeconfigHeader(base?: HttpHeaders): HttpHeaders {
  let headers = base ?? new HttpHeaders();
  const kubeconfig = localStorage.getItem(KUBECONFIG_KEY);
  if (!kubeconfig || headers.has('X-Kubeconfig-B64')) {
    return headers;
  }

  try {
    const kubeconfigB64 = btoa(unescape(encodeURIComponent(kubeconfig)));
    headers = headers.set('X-Kubeconfig-B64', kubeconfigB64);
  } catch (error) {
    console.error('[kubeconfig-header] encode kubeconfig failed', error);
  }
  return headers;
}

export function buildJsonHeaders(): HttpHeaders {
  const headers = new HttpHeaders({ 'Content-Type': 'application/json' });
  return appendKubeconfigHeader(headers);
}
