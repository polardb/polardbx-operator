package api

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	apiutil "polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"
)

var newAllClientsFromKubeconfig = k8s.NewAllClientsFromKubeconfig

func normalizeKubeconfig(raw []byte) ([]byte, *clientcmdapi.Config, error) {
	cfg, err := clientcmd.Load(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse kubeconfig: %w", err)
	}
	if cfg == nil {
		return nil, nil, fmt.Errorf("kubeconfig is empty")
	}

	for name, cluster := range cfg.Clusters {
		if cluster == nil {
			continue
		}
		if len(cluster.CertificateAuthorityData) == 0 && cluster.CertificateAuthority != "" {
			log.Printf("normalizeKubeconfig: inlining certificate authority for cluster=%s from path=%s", name, cluster.CertificateAuthority)
			data, err := readCredentialFile(cluster.CertificateAuthority)
			if err != nil {
				return nil, nil, fmt.Errorf("cluster %q certificate-authority %q: %w", name, cluster.CertificateAuthority, err)
			}
			cluster.CertificateAuthorityData = data
			cluster.CertificateAuthority = ""
		}
	}

	for name, authInfo := range cfg.AuthInfos {
		if authInfo == nil {
			continue
		}
		if len(authInfo.ClientCertificateData) == 0 && authInfo.ClientCertificate != "" {
			log.Printf("normalizeKubeconfig: inlining client certificate for user=%s from path=%s", name, authInfo.ClientCertificate)
			data, err := readCredentialFile(authInfo.ClientCertificate)
			if err != nil {
				return nil, nil, fmt.Errorf("user %q client-certificate %q: %w", name, authInfo.ClientCertificate, err)
			}
			authInfo.ClientCertificateData = data
			authInfo.ClientCertificate = ""
		}
		if len(authInfo.ClientKeyData) == 0 && authInfo.ClientKey != "" {
			log.Printf("normalizeKubeconfig: inlining client key for user=%s from path=%s", name, authInfo.ClientKey)
			data, err := readCredentialFile(authInfo.ClientKey)
			if err != nil {
				return nil, nil, fmt.Errorf("user %q client-key %q: %w", name, authInfo.ClientKey, err)
			}
			authInfo.ClientKeyData = data
			authInfo.ClientKey = ""
		}
	}

	normalized, err := clientcmd.Write(*cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to serialize kubeconfig: %w", err)
	}

	return normalized, cfg, nil
}

func readCredentialFile(path string) ([]byte, error) {
	if path == "" {
		return nil, fmt.Errorf("credential path is empty")
	}

	expanded := os.ExpandEnv(path)
	if strings.HasPrefix(expanded, "~") {
		if home, err := os.UserHomeDir(); err == nil {
			switch {
			case expanded == "~":
				expanded = home
			case strings.HasPrefix(expanded, "~/"):
				expanded = filepath.Join(home, expanded[2:])
			case strings.HasPrefix(expanded, "~"+string(os.PathSeparator)):
				expanded = filepath.Join(home, expanded[2:])
			}
		}
	}

	expanded = filepath.Clean(expanded)

	data, err := os.ReadFile(expanded)
	if err != nil {
		return nil, fmt.Errorf("read credential file %q: %w", expanded, err)
	}
	return data, nil
}

// KubeconfigAuthMiddleware validates the provided kubeconfig from the request header.
func KubeconfigAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestPath := c.FullPath()
		if requestPath == "" {
			requestPath = c.Request.URL.Path
		}
		log.Printf("KubeconfigAuthMiddleware: handling %s %s from %s", c.Request.Method, requestPath, c.ClientIP())

		kubeconfigB64 := c.GetHeader("X-Kubeconfig-B64")
		// WebSocket 等场景无法自定义 Header 时，允许通过查询参数传递
		if kubeconfigB64 == "" {
			if v := c.Query("kubeconfig"); v != "" {
				kubeconfigB64 = v
			}
		}
		if kubeconfigB64 == "" {
			if v := c.Query("k"); v != "" {
				kubeconfigB64 = v
			}
		}
		if kubeconfigB64 == "" {
			log.Printf("KubeconfigAuthMiddleware: missing kubeconfig for %s from %s", requestPath, c.ClientIP())
			c.JSON(http.StatusUnauthorized, gin.H{"error": "kubeconfig not provided"})
			c.Abort()
			return
		}

		// Decode the base64 kubeconfig
		kubeconfig, err := base64.StdEncoding.DecodeString(kubeconfigB64)
		if err != nil {
			log.Printf("KubeconfigAuthMiddleware: base64 decode failed for %s from %s: %v", requestPath, c.ClientIP(), err)
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid kubeconfig base64", "details": err.Error()})
			c.Abort()
			return
		}

		normalized, cfg, err := normalizeKubeconfig(kubeconfig)
		if err != nil {
			log.Printf("KubeconfigAuthMiddleware: normalize failed for %s from %s: %v", requestPath, c.ClientIP(), err)
			status := http.StatusBadRequest
			switch {
			case errors.Is(err, fs.ErrPermission):
				status = http.StatusForbidden
			case errors.Is(err, fs.ErrNotExist):
				status = http.StatusBadRequest
			}
			c.JSON(status, gin.H{"error": "failed to normalize kubeconfig", "details": err.Error()})
			c.Abort()
			return
		}

		// Create Kubernetes clients with normalized kubeconfig
		ctrlClient, clientset, dynClient, err := newAllClientsFromKubeconfig(normalized)
		if err != nil {
			log.Printf("KubeconfigAuthMiddleware: client creation failed for %s from %s: %v", requestPath, c.ClientIP(), err)
			c.JSON(http.StatusUnauthorized, gin.H{"error": "failed to create kubernetes clients", "details": err.Error()})
			c.Abort()
			return
		}

		c.Set("k8sClient", ctrlClient)
		c.Set("clientset", clientset)
		c.Set("dynamic-client", dynClient)

		// Extract identity for audit (best-effort)
		if cfg != nil {
			ctxName := cfg.CurrentContext
			user := ctxName
			if ctx, ok := cfg.Contexts[ctxName]; ok && ctx != nil {
				if ctx.AuthInfo != "" {
					user = ctx.AuthInfo
				}
				if ctx.Namespace != "" {
					c.Set("k8sDefaultNamespace", ctx.Namespace)
				}
			}
			c.Set("k8sUser", user)
			c.Set("k8sContext", ctxName)
			log.Printf("KubeconfigAuthMiddleware: authenticated context=%s user=%s namespace=%s for %s", ctxName, user, c.GetString("k8sDefaultNamespace"), requestPath)
		}
		c.Set("normalizedKubeconfig", normalized)
		log.Printf("KubeconfigAuthMiddleware: kubeconfig normalized and clients stored for %s", requestPath)
		c.Next()
	}
}

// Connect handler validates the provided kubeconfig from the request header.
func Connect(c *gin.Context) {
	cs, ok := apiutil.ClientsetFromContext(c)
	if !ok {
		log.Printf("Connect: clientset missing for request from %s", c.ClientIP())
		c.JSON(http.StatusUnauthorized, gin.H{"error": "kubeconfig not provided or invalid"})
		return
	}

	defNs := ""
	if v, ok := c.Get("k8sDefaultNamespace"); ok {
		if s, ok2 := v.(string); ok2 {
			defNs = s
		}
	}
	user := ""
	if v, ok := c.Get("k8sUser"); ok {
		if s, ok2 := v.(string); ok2 {
			user = s
		}
	}
	ctxName := ""
	if v, ok := c.Get("k8sContext"); ok {
		if s, ok2 := v.(string); ok2 {
			ctxName = s
		}
	}

	log.Printf("Connect: verifying access for context=%s user=%s namespace=%s from %s", ctxName, user, defNs, c.ClientIP())

	resp := gin.H{
		"message":          "connection successful",
		"user":             user,
		"context":          ctxName,
		"defaultNamespace": defNs,
	}

	// 1) 与 apiserver 通信
	log.Printf("Connect: querying apiserver version for context=%s user=%s", ctxName, user)
	sv, err := cs.Discovery().ServerVersion()
	if err != nil {
		log.Printf("Connect: server version query failed for context=%s user=%s: %T %v", ctxName, user, err, err)
		switch {
		case k8serrors.IsUnauthorized(err):
			c.JSON(http.StatusUnauthorized, gin.H{"error": "authentication failed", "details": err.Error()})
		case k8serrors.IsForbidden(err):
			c.JSON(http.StatusForbidden, gin.H{"error": "permission denied", "details": err.Error()})
		default:
			var netErr net.Error
			if errors.As(err, &netErr) {
				payload := gin.H{"error": "apiserver unreachable", "details": err.Error()}
				if netErr.Timeout() {
					payload["reason"] = "timeout"
				}
				c.JSON(http.StatusGatewayTimeout, payload)
			} else {
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "apiserver unreachable", "details": err.Error()})
			}
		}
		return
	}
	log.Printf("Connect: apiserver version query succeeded for context=%s user=%s", ctxName, user)

	// 2) RBAC 轻量校验：列出命名空间（限制 1）
	if _, err := cs.CoreV1().Namespaces().List(c.Request.Context(), metav1.ListOptions{Limit: 1}); err != nil {
		log.Printf("Connect: namespace list failed for context=%s user=%s: %v", ctxName, user, err)
		apiutil.HandleK8sError(c, "failed to list namespaces", err)
		return
	}

	if sv != nil {
		resp["apiserverVersion"] = sv.GitVersion
		resp["platform"] = sv.Platform
		log.Printf("Connect: apiserverVersion=%s platform=%s context=%s user=%s", sv.GitVersion, sv.Platform, ctxName, user)
	}

	c.JSON(http.StatusOK, resp)
	log.Printf("Connect: connection successful for context=%s user=%s", ctxName, user)
}

// Note: error handling and client getters are centralized in api/util.
