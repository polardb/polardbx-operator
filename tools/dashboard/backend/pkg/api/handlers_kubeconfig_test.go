package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/clientcmd"
)

func TestNormalizeKubeconfigRejectsFileReferences(t *testing.T) {
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
	assert.True(t, errors.Is(err, ErrUnsafeKubeconfig), "expected unsafe kubeconfig error, got %v", err)
}

func TestNormalizeKubeconfigAllowsInlineDataFields(t *testing.T) {
	kubeconfigYAML := `apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: Q0EtREFUQQ==
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
    client-certificate-data: Q0VSVC1EQVRB
    client-key-data: S0VZLURBVEE=
`

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
}
