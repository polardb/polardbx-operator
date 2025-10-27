package api

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
)

func TestNormalizeKubeconfigInlinesFileReferences(t *testing.T) {
	dir := t.TempDir()

	caFile := filepath.Join(dir, "ca.crt")
	certFile := filepath.Join(dir, "client.crt")
	keyFile := filepath.Join(dir, "client.key")

	require.NoError(t, os.WriteFile(caFile, []byte("CA-DATA"), 0o600))
	require.NoError(t, os.WriteFile(certFile, []byte("CERT-DATA"), 0o600))
	require.NoError(t, os.WriteFile(keyFile, []byte("KEY-DATA"), 0o600))

	kubeconfigYAML := fmt.Sprintf(`apiVersion: v1
clusters:
- cluster:
    certificate-authority: %s
    server: https://127.0.0.1:8443
  name: test
contexts:
- context:
    cluster: test
    user: user
  name: test
current-context: test
kind: Config
preferences: {}
users:
- name: user
  user:
    client-certificate: %s
    client-key: %s
`, caFile, certFile, keyFile)

	normalized, cfg, err := normalizeKubeconfig([]byte(kubeconfigYAML))
	require.NoError(t, err)
	require.NotNil(t, cfg)

	cluster := cfg.Clusters["test"]
	require.NotNil(t, cluster)
	assert.Empty(t, cluster.CertificateAuthority)
	assert.Equal(t, []byte("CA-DATA"), cluster.CertificateAuthorityData)

	user := cfg.AuthInfos["user"]
	require.NotNil(t, user)
	assert.Empty(t, user.ClientCertificate)
	assert.Empty(t, user.ClientKey)
	assert.Equal(t, []byte("CERT-DATA"), user.ClientCertificateData)
	assert.Equal(t, []byte("KEY-DATA"), user.ClientKeyData)

	parsedNormalized, err := clientcmd.Load(normalized)
	require.NoError(t, err)
	normalizedCluster := parsedNormalized.Clusters["test"]
	require.NotNil(t, normalizedCluster)
	assert.Empty(t, normalizedCluster.CertificateAuthority)
	assert.NotEmpty(t, normalizedCluster.CertificateAuthorityData)
	assert.NotContains(t, string(normalized), caFile)
}

func TestNormalizeKubeconfigMissingFile(t *testing.T) {
	kubeconfigYAML := `apiVersion: v1
clusters:
- cluster:
    certificate-authority: /path/does/not/exist.crt
    server: https://127.0.0.1:8443
  name: missing
contexts:
- context:
    cluster: missing
    user: user
  name: missing
current-context: missing
kind: Config
preferences: {}
users:
- name: user
  user:
    client-key: /path/also/missing.key
`

	_, _, err := normalizeKubeconfig([]byte(kubeconfigYAML))
	require.Error(t, err)
	assert.True(t, errors.Is(err, fs.ErrNotExist), "expected not-exist error, got %v", err)
}
