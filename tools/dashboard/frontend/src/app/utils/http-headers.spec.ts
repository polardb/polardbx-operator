import { appendKubeconfigHeader, buildJsonHeaders } from './http-headers';

describe('http-headers utilities', () => {
  const kubeconfig = 'apiVersion: v1\nkind: Config\nclusters:\n- name: test';
  const encoded = btoa(unescape(encodeURIComponent(kubeconfig)));

  beforeEach(() => {
    localStorage.clear();
  });

  it('buildJsonHeaders should set JSON content type', () => {
    const headers = buildJsonHeaders();
    expect(headers.get('Content-Type')).toBe('application/json');
  });

  it('buildJsonHeaders should append kubeconfig when available', () => {
    localStorage.setItem('kubeconfig', kubeconfig);

    const headers = buildJsonHeaders();

    expect(headers.get('X-Kubeconfig-B64')).toBe(encoded);
  });

  it('appendKubeconfigHeader should not override existing kubeconfig header', () => {
    localStorage.setItem('kubeconfig', kubeconfig);

    const existingHeaders = buildJsonHeaders().set('X-Kubeconfig-B64', 'custom');
    const headers = appendKubeconfigHeader(existingHeaders);

    expect(headers.get('X-Kubeconfig-B64')).toBe('custom');
  });

  it('appendKubeconfigHeader should leave headers unchanged when kubeconfig missing', () => {
    const base = buildJsonHeaders();
    const headers = appendKubeconfigHeader(base);

    expect(headers).toBe(base);
  });
});
