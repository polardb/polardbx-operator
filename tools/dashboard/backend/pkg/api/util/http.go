package util

import (
	"context"
	"encoding/base64"
	"time"

	"github.com/gin-gonic/gin"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apierr "polardbx-dashboard-backend/pkg/api/errors"
	"polardbx-dashboard-backend/pkg/k8s"
	"polardbx-dashboard-backend/pkg/logger"

	"k8s.io/client-go/tools/clientcmd"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

// K8sClientFromContext returns controller-runtime client from gin context.
func K8sClientFromContext(c *gin.Context) (client.Client, bool) {
	v, ok := c.Get("k8sClient")
	if !ok {
		apierr.AbortUnauthorized(c, "Kubernetes client not initialized")
		return nil, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		apierr.AbortUnauthorized(c, "Invalid kubernetes client in context")
		return nil, false
	}
	return cli, true
}

// ClientsetFromContext returns client-go clientset from gin context.
func ClientsetFromContext(c *gin.Context) (kubernetes.Interface, bool) {
	v, ok := c.Get("clientset")
	if !ok {
		return nil, false
	}
	cs, ok := v.(kubernetes.Interface)
	if !ok || cs == nil {
		return nil, false
	}
	return cs, true
}

// DynamicClientFromContext returns dynamic client from gin context.
func DynamicClientFromContext(c *gin.Context) (dynamic.Interface, bool) {
	v, ok := c.Get("dynamic-client")
	if !ok {
		apierr.AbortWithError(c, apierr.InternalServiceError("Kubernetes dynamic client not available", nil))
		return nil, false
	}
	dynClient, ok := v.(dynamic.Interface)
	if !ok || dynClient == nil {
		apierr.AbortWithError(c, apierr.InternalServiceError("Invalid dynamic client in context", nil))
		return nil, false
	}
	return dynClient, true
}

// GetK8sClients returns ctrl-runtime client, clientset and dynamic client together.
// It aborts with proper apierr responses when any client is missing.
func GetK8sClients(c *gin.Context) (client.Client, kubernetes.Interface, dynamic.Interface, bool) {
	cli, ok := K8sClientFromContext(c)
	if !ok {
		return nil, nil, nil, false
	}
	cs, ok := ClientsetFromContext(c)
	if !ok {
		apierr.AbortUnauthorized(c, "Kubernetes clientset not initialized")
		return nil, nil, nil, false
	}
	dyn, ok := DynamicClientFromContext(c)
	if !ok {
		return nil, nil, nil, false
	}
	return cli, cs, dyn, true
}

// DefaultNamespace returns query namespace or the middleware-injected default.
func DefaultNamespace(c *gin.Context, fallback string) string {
	if ns := c.Query("namespace"); ns != "" {
		return ns
	}
	if v, ok := c.Get("k8sDefaultNamespace"); ok {
		if s, ok2 := v.(string); ok2 && s != "" {
			return s
		}
	}
	return fallback
}

// GetNamespace tries path param first, then query, then fallback or injected default namespace.
func GetNamespace(c *gin.Context, fallback string) string {
	if ns := c.Param("namespace"); ns != "" {
		return ns
	}
	if ns := c.Query("namespace"); ns != "" {
		return ns
	}
	return DefaultNamespace(c, fallback)
}

// HandleK8sError maps common k8s errors to HTTP codes using unified error handling.
// It logs the full error internally and returns a sanitized response to the client.
func HandleK8sError(c *gin.Context, operation string, err error) {
	// Log full error details internally using structured logger
	user := c.GetString("k8sUser")
	requestID := c.GetString("requestId")
	logger.Error("K8s operation failed",
		"operation", operation,
		"user", user,
		"requestId", requestID,
		"error", err)

	// Use the unified error handler which sanitizes the response
	apierr.AbortK8sError(c, operation, err)
}

// HandleK8sErrorLegacy is the old implementation - kept for reference during migration
// Deprecated: Use HandleK8sError instead
func HandleK8sErrorLegacy(c *gin.Context, context string, err error) {
	// Delegate to the unified error handler
	apierr.AbortK8sError(c, context, err)
}

// ---- Handler timeout helpers ----

const (
	DefaultListTimeout = 15 * time.Second
	DefaultCRUDTimeout = 60 * time.Second
)

func ListCtx(c *gin.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Request.Context(), DefaultListTimeout)
}

func CrudCtx(c *gin.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.Request.Context(), DefaultCRUDTimeout)
}

// BindValidateAndCtx binds JSON to dst, runs optional validators, and returns ctx with timeout.
// On validation error it aborts and returns ok=false.
func BindValidateAndCtx(c *gin.Context, dst interface{}, timeout time.Duration, validators ...func(interface{}) error) (context.Context, context.CancelFunc, bool) {
	if err := c.ShouldBindJSON(dst); err != nil {
		// Let ConvertServiceError normalize binding/validation errors (field-level details when available).
		apierr.AbortWithError(c, err)
		return nil, nil, false
	}
	for _, validate := range validators {
		if validate == nil {
			continue
		}
		if err := validate(dst); err != nil {
			// Treat validator failures as user input validation errors by default.
			if _, ok := err.(*apierr.ServiceError); ok {
				apierr.AbortWithError(c, err)
				return nil, nil, false
			}
			apierr.AbortWithError(c, apierr.ValidationError(err.Error(), nil))
			return nil, nil, false
		}
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	return ctx, cancel, true
}

// ExtractKubeconfigB64 extracts kubeconfig (base64) from header/query/body.
func ExtractKubeconfigB64(c *gin.Context) (string, bool) {
	kubeconfigB64 := c.GetHeader("X-Kubeconfig-B64")
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
	if kubeconfigB64 == "" && c.Request.Body != nil {
		var payload struct {
			Kubeconfig string `json:"kubeconfig"`
		}
		_ = c.ShouldBindJSON(&payload)
		if payload.Kubeconfig != "" {
			kubeconfigB64 = payload.Kubeconfig
		}
	}
	if kubeconfigB64 == "" {
		return "", false
	}
	return kubeconfigB64, true
}

// InitClientsFromKubeconfigB64 decodes and initializes clients, injects into context.
func InitClientsFromKubeconfigB64(c *gin.Context, kubeconfigB64 string) (client.Client, kubernetes.Interface, error) {
	cfgBytes, err := base64.StdEncoding.DecodeString(kubeconfigB64)
	if err != nil {
		return nil, nil, err
	}
	ctrlClient, clientset, dynClient, err := k8s.NewAllClientsFromKubeconfig(cfgBytes)
	if err != nil {
		return nil, nil, err
	}
	c.Set("k8sClient", ctrlClient)
	c.Set("clientset", clientset)
	c.Set("dynamic-client", dynClient)
	if loaded, e := clientcmd.Load(cfgBytes); e == nil && loaded != nil {
		ctxName := loaded.CurrentContext
		user := ctxName
		if ctx, ok := loaded.Contexts[ctxName]; ok && ctx != nil {
			if ctx.AuthInfo != "" {
				user = ctx.AuthInfo
			}
			if ctx.Namespace != "" {
				c.Set("k8sDefaultNamespace", ctx.Namespace)
			}
		}
		c.Set("k8sUser", user)
		c.Set("k8sContext", ctxName)
	}
	return ctrlClient, clientset, nil
}

// NotFound is a helper for deprecated endpoints after migration.
func NotFound(c *gin.Context) {
	apierr.AbortWithError(c, apierr.NotFoundError("endpoint", "deprecated - use new domain handlers"))
}

// K8sPatchClusterJSON is a small wrapper to patch PolarDBXCluster with raw JSON merge patch.
func K8sPatchClusterJSON(ctx context.Context, cli client.Client, namespace, name string, patch []byte) (*polardbxv1.PolarDBXCluster, error) {
	return k8s.PatchPolarDBXClusterWithContext(ctx, cli, namespace, name, patch)
}
