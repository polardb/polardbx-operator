package util

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	LogsSecurityConfigMapName      = "logs-config"
	LogsSecurityConfigMapNamespace = "polardbx-logcollector"
	LogsSecurityAllowedHostsKey    = "allowedHosts"
	LogsSecurityDefaultHostKey     = "defaultHost"
	ElasticsearchCredsSecret       = "elastic-credentials"
	ElasticsearchCertsSecret       = "elastic-certs-public"
)

// LoadLogsSecurityConfig returns allowed hosts and default host from ConfigMap.
func LoadLogsSecurityConfig(c *gin.Context) (allowedHosts []string, defaultHost string, err error) {
	cs, ok := ClientsetFromContext(c)
	if !ok {
		return nil, "", fmt.Errorf("no clientset")
	}
	cm, e := cs.CoreV1().ConfigMaps(LogsSecurityConfigMapNamespace).Get(c.Request.Context(), LogsSecurityConfigMapName, metav1.GetOptions{})
	if e != nil {
		return nil, "", e
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	allowed := strings.TrimSpace(cm.Data[LogsSecurityAllowedHostsKey])
	if allowed != "" {
		for _, h := range strings.Split(allowed, ",") {
			h = strings.TrimSpace(h)
			if h != "" {
				allowedHosts = append(allowedHosts, h)
			}
		}
	}
	defaultHost = strings.TrimSpace(cm.Data[LogsSecurityDefaultHostKey])
	return allowedHosts, defaultHost, nil
}

// LoadESCredentials returns username and password from Secret; ok is false if not found.
func LoadESCredentials(c *gin.Context) (username, password string, ok bool, err error) {
	cs, ok2 := ClientsetFromContext(c)
	if !ok2 {
		return "", "", false, fmt.Errorf("no clientset")
	}
	sec, e := cs.CoreV1().Secrets(LogsSecurityConfigMapNamespace).Get(c.Request.Context(), ElasticsearchCredsSecret, metav1.GetOptions{})
	if e != nil {
		return "", "", false, e
	}
	return string(sec.Data["username"]), string(sec.Data["password"]), true, nil
}

// LoadESRootCAs returns a cert pool from elastic-certs-public's ca.crt if present; nil if not present or invalid.
func LoadESRootCAs(c *gin.Context) (*x509.CertPool, error) {
	cs, ok := ClientsetFromContext(c)
	if !ok {
		return nil, fmt.Errorf("no clientset")
	}
	sec, err := cs.CoreV1().Secrets(LogsSecurityConfigMapNamespace).Get(c.Request.Context(), ElasticsearchCertsSecret, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	crt := sec.Data["ca.crt"]
	if len(crt) == 0 {
		return nil, fmt.Errorf("no ca.crt in secret")
	}
	blk, _ := pem.Decode(crt)
	if blk == nil || blk.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("invalid pem")
	}
	cert, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return pool, nil
}

// IsHostAllowed checks if host in allowed list. If whitelist is empty, deny unless default matches.
func IsHostAllowed(host string, allowed []string, defaultHost string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if len(allowed) == 0 {
		return defaultHost != "" && host == defaultHost
	}
	for _, a := range allowed {
		if host == a {
			return true
		}
	}
	return false
}

// EnsureLogsSecurityBootstrap ensures the ConfigMap exists with minimal defaults.
func EnsureLogsSecurityBootstrap(c *gin.Context) error {
	cs, ok := ClientsetFromContext(c)
	if !ok {
		return fmt.Errorf("no clientset")
	}
	_, err := cs.CoreV1().ConfigMaps(LogsSecurityConfigMapNamespace).Get(c.Request.Context(), LogsSecurityConfigMapName, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: LogsSecurityConfigMapName, Namespace: LogsSecurityConfigMapNamespace}, Data: map[string]string{LogsSecurityAllowedHostsKey: "", LogsSecurityDefaultHostKey: ""}}
	_, e := cs.CoreV1().ConfigMaps(LogsSecurityConfigMapNamespace).Create(c.Request.Context(), cm, metav1.CreateOptions{})
	return e
}
