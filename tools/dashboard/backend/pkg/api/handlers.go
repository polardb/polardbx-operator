package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/gin-gonic/gin"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	apiutil "polardbx-dashboard-backend/pkg/api/util"
	"polardbx-dashboard-backend/pkg/k8s"
	"polardbx-dashboard-backend/pkg/logger"
)

var newAllClientsFromKubeconfig = k8s.NewAllClientsFromKubeconfig

var ErrUnsafeKubeconfig = errors.New("unsafe kubeconfig")

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
		if cluster.CertificateAuthority != "" {
			return nil, nil, fmt.Errorf("%w: kubeconfig cluster %q uses certificate-authority file path; use certificate-authority-data (e.g. kubectl config view --raw)", ErrUnsafeKubeconfig, name)
		}
	}

	for name, authInfo := range cfg.AuthInfos {
		if authInfo == nil {
			continue
		}
		if authInfo.ClientCertificate != "" {
			return nil, nil, fmt.Errorf("%w: kubeconfig user %q uses client-certificate file path; use client-certificate-data (e.g. kubectl config view --raw)", ErrUnsafeKubeconfig, name)
		}
		if authInfo.ClientKey != "" {
			return nil, nil, fmt.Errorf("%w: kubeconfig user %q uses client-key file path; use client-key-data (e.g. kubectl config view --raw)", ErrUnsafeKubeconfig, name)
		}
		if authInfo.TokenFile != "" {
			return nil, nil, fmt.Errorf("%w: kubeconfig user %q uses tokenFile; inline token instead", ErrUnsafeKubeconfig, name)
		}
		if authInfo.Exec != nil {
			return nil, nil, fmt.Errorf("%w: kubeconfig user %q uses exec auth plugin which is not allowed", ErrUnsafeKubeconfig, name)
		}
		if authInfo.AuthProvider != nil {
			return nil, nil, fmt.Errorf("%w: kubeconfig user %q uses auth-provider which is not allowed", ErrUnsafeKubeconfig, name)
		}
	}

	normalized, err := clientcmd.Write(*cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to serialize kubeconfig: %w", err)
	}

	return normalized, cfg, nil
}

// KubeconfigAuthMiddleware validates the provided kubeconfig from the request header.
func KubeconfigAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestPath := c.FullPath()
		if requestPath == "" {
			requestPath = c.Request.URL.Path
		}
		logger.Info("KubeconfigAuthMiddleware: handling request",
			"method", c.Request.Method,
			"path", requestPath,
			"clientIP", c.ClientIP())

		kubeconfigB64 := c.GetHeader("X-Kubeconfig-B64")
		// Allow passing via query parameter when custom headers cannot be set (e.g., WebSocket scenarios)
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
			logger.Warn("KubeconfigAuthMiddleware: missing kubeconfig",
				"path", requestPath,
				"clientIP", c.ClientIP())
			apierr.AbortUnauthorized(c, "kubeconfig not provided")
			return
		}

		// Decode the base64 kubeconfig
		kubeconfig, err := base64.StdEncoding.DecodeString(kubeconfigB64)
		if err != nil {
			logger.Error("KubeconfigAuthMiddleware: base64 decode failed",
				"path", requestPath,
				"clientIP", c.ClientIP(),
				"error", err)
			apierr.Abort(c, apierr.Validation("invalid kubeconfig base64"))
			return
		}

		normalized, cfg, err := normalizeKubeconfig(kubeconfig)
		if err != nil {
			logger.Error("KubeconfigAuthMiddleware: normalize failed",
				"path", requestPath,
				"clientIP", c.ClientIP(),
				"error", err)
			if errors.Is(err, ErrUnsafeKubeconfig) {
				apierr.Abort(c, apierr.Validation(err.Error()))
				return
			}
			apierr.Abort(c, apierr.Validation("failed to parse kubeconfig"))
			return
		}

		// Create Kubernetes clients with normalized kubeconfig (cached by kubeconfig hash)
		clients, cacheHit, err := apiutil.GetOrCreateK8sClientsFromKubeconfig(normalized, newAllClientsFromKubeconfig)
		if err != nil {
			logger.Error("KubeconfigAuthMiddleware: client creation failed",
				"path", requestPath,
				"clientIP", c.ClientIP(),
				"error", err)
			apierr.AbortUnauthorized(c, "failed to create kubernetes clients")
			return
		}

		if cacheHit {
			logger.Debug("KubeconfigAuthMiddleware: reused cached kubernetes clients",
				"path", requestPath)
		}

		c.Set("k8sClient", clients.Client)
		c.Set("clientset", clients.Clientset)
		c.Set("dynamic-client", clients.Dynamic)

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
			logger.Info("KubeconfigAuthMiddleware: authenticated",
				"context", ctxName,
				"user", user,
				"namespace", c.GetString("k8sDefaultNamespace"),
				"path", requestPath)
		}
		c.Set("normalizedKubeconfig", normalized)
		logger.Info("KubeconfigAuthMiddleware: kubeconfig normalized and clients stored",
			"path", requestPath)
		c.Next()
	}
}

// Connect handler validates the provided kubeconfig from the request header.
func Connect(c *gin.Context) {
	cs, ok := apiutil.ClientsetFromContext(c)
	if !ok {
		logger.Warn("Connect: clientset missing",
			"clientIP", c.ClientIP())
		apierr.AbortUnauthorized(c, "kubeconfig not provided or invalid")
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

	logger.Info("Connect: verifying access",
		"context", ctxName,
		"user", user,
		"namespace", defNs,
		"clientIP", c.ClientIP())

	resp := gin.H{
		"message":          "connection successful",
		"user":             user,
		"context":          ctxName,
		"defaultNamespace": defNs,
	}

	// Create a context with timeout for API server operations
	connectTimeout := 10 * time.Second
	connectCtx, cancel := context.WithTimeout(c.Request.Context(), connectTimeout)
	defer cancel()

	// 1) Communicate with apiserver using RESTClient with context timeout
	logger.Info("Connect: querying apiserver version",
		"context", ctxName,
		"user", user,
		"timeout", connectTimeout)

	// Use RESTClient with context for timeout control
	versionResult := cs.Discovery().RESTClient().Get().AbsPath("/version").Do(connectCtx)
	if versionResult.Error() != nil {
		err := versionResult.Error()
		logger.Error("Connect: server version query failed",
			"context", ctxName,
			"user", user,
			"error", err)

		// Check if context was cancelled due to timeout
		if connectCtx.Err() == context.DeadlineExceeded {
			logger.Error("Connect: apiserver connection timeout",
				"context", ctxName,
				"user", user,
				"timeout", connectTimeout)
			apierr.Abort(c, apierr.Timeout("apiserver connection timeout - check network connectivity and kubeconfig server address"))
			return
		}

		switch {
		case k8serrors.IsUnauthorized(err):
			apierr.AbortUnauthorized(c, "authentication failed")
		case k8serrors.IsForbidden(err):
			apierr.AbortForbidden(c, "permission denied")
		default:
			var netErr net.Error
			if errors.As(err, &netErr) {
				apiErr := apierr.Timeout("apiserver unreachable - check network connectivity and kubeconfig server address")
				if !netErr.Timeout() {
					apiErr = apierr.ServiceUnavailable("apiserver unreachable - check network connectivity and kubeconfig server address", 0)
				}
				apierr.Abort(c, apiErr)
			} else {
				apierr.Abort(c, apierr.ServiceUnavailable("apiserver unreachable - check network connectivity and kubeconfig server address", 0))
			}
		}
		return
	}

	// Parse version info from raw response
	var sv *version.Info
	raw, err := versionResult.Raw()
	if err == nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, &sv); err != nil {
			logger.Warn("Connect: failed to parse version response, continuing without version info",
				"context", ctxName,
				"user", user,
				"error", err)
			sv = nil
		} else {
			logger.Info("Connect: apiserver version query succeeded",
				"context", ctxName,
				"user", user)
		}
	} else {
		logger.Warn("Connect: failed to get version raw response, continuing without version info",
			"context", ctxName,
			"user", user,
			"error", err)
		sv = nil
	}

	// 2) Lightweight RBAC validation: list namespaces (limit 1)
	if _, err := cs.CoreV1().Namespaces().List(connectCtx, metav1.ListOptions{Limit: 1}); err != nil {
		logger.Error("Connect: namespace list failed",
			"context", ctxName,
			"user", user,
			"error", err)

		// Check if context was cancelled due to timeout
		if connectCtx.Err() == context.DeadlineExceeded {
			logger.Error("Connect: namespace list timeout",
				"context", ctxName,
				"user", user,
				"timeout", connectTimeout)
			apierr.Abort(c, apierr.Timeout("apiserver connection timeout - check network connectivity"))
			return
		}

		apierr.AbortWithError(c, err)
		return
	}

	if sv != nil {
		resp["apiserverVersion"] = sv.GitVersion
		resp["platform"] = sv.Platform
		logger.Info("Connect: apiserver info",
			"apiserverVersion", sv.GitVersion,
			"platform", sv.Platform,
			"context", ctxName,
			"user", user)
	}

	apierr.OK(c, resp)
	logger.Info("Connect: connection successful",
		"context", ctxName,
		"user", user)
}

// Note: error handling and client getters are centralized in api/util.
