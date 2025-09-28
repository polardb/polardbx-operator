package util

import (
	"context"
	"encoding/base64"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"polardbx-ui-backend/pkg/k8s"

	"k8s.io/client-go/tools/clientcmd"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
)

// K8sClientFromContext returns controller-runtime client from gin context.
func K8sClientFromContext(c *gin.Context) (client.Client, bool) {
	v, ok := c.Get("k8sClient")
	if !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "kubernetes client not initialized"})
		return nil, false
	}
	cli, ok := v.(client.Client)
	if !ok || cli == nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid kubernetes client in context"})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": "kubernetes dynamic client not available"})
		return nil, false
	}
	dynClient, ok := v.(dynamic.Interface)
	if !ok || dynClient == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "invalid dynamic client in context"})
		return nil, false
	}
	return dynClient, true
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

// HandleK8sError maps common k8s errors to HTTP codes.
func HandleK8sError(c *gin.Context, context string, err error) {
	switch {
	case k8serrors.IsInvalid(err), k8serrors.IsBadRequest(err):
		c.JSON(http.StatusBadRequest, gin.H{"error": context, "details": err.Error()})
	case k8serrors.IsAlreadyExists(err):
		c.JSON(http.StatusConflict, gin.H{"error": context, "details": err.Error()})
	case k8serrors.IsNotFound(err):
		c.JSON(http.StatusNotFound, gin.H{"error": context, "details": err.Error()})
	case k8serrors.IsForbidden(err):
		c.JSON(http.StatusForbidden, gin.H{"error": context, "details": err.Error()})
	case k8serrors.IsUnauthorized(err):
		c.JSON(http.StatusUnauthorized, gin.H{"error": context, "details": err.Error()})
	case k8serrors.IsTimeout(err):
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": context, "details": err.Error()})
	case k8serrors.IsTooManyRequests(err):
		c.JSON(http.StatusTooManyRequests, gin.H{"error": context, "details": err.Error()})
	case k8serrors.IsServiceUnavailable(err):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": context, "details": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": context, "details": err.Error()})
	}
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
	c.JSON(http.StatusNotFound, gin.H{"error": "endpoint deprecated", "details": "use new domain handlers"})
}

// K8sPatchClusterJSON is a small wrapper to patch PolarDBXCluster with raw JSON merge patch.
func K8sPatchClusterJSON(ctx context.Context, cli client.Client, namespace, name string, patch []byte) (*polardbxv1.PolarDBXCluster, error) {
	return k8s.PatchPolarDBXClusterWithContext(ctx, cli, namespace, name, patch)
}
