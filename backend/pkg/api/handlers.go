package api

import (
	"encoding/base64"
	"net/http"

	"github.com/gin-gonic/gin"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	apiutil "polardbx-ui-backend/pkg/api/util"
	"polardbx-ui-backend/pkg/k8s"
)

// KubeconfigAuthMiddleware validates the provided kubeconfig from the request header.
func KubeconfigAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
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
			c.JSON(http.StatusUnauthorized, gin.H{"error": "kubeconfig not provided"})
			c.Abort()
			return
		}

		// Decode the base64 kubeconfig
		kubeconfig, err := base64.StdEncoding.DecodeString(kubeconfigB64)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid kubeconfig base64", "details": err.Error()})
			c.Abort()
			return
		}

		// Create Kubernetes clients
		ctrlClient, clientset, dynClient, err := k8s.NewAllClientsFromKubeconfig(kubeconfig)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "failed to create kubernetes clients", "details": err.Error()})
			c.Abort()
			return
		}

		c.Set("k8sClient", ctrlClient)
		c.Set("clientset", clientset)
		c.Set("dynamic-client", dynClient)

		// Extract identity for audit (best-effort)
		if cfg, err := clientcmd.Load(kubeconfig); err == nil && cfg != nil {
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
		}
		c.Next()
	}
}

// Connect handler validates the provided kubeconfig from the request header.
func Connect(c *gin.Context) {
	// Prefer clientset from context (when middleware applied)
	var cs kubernetes.Interface
	if v, ok := c.Get("clientset"); ok {
		if vv, ok2 := v.(kubernetes.Interface); ok2 {
			cs = vv
		}
	}

	// If not present, try to build from kubeconfig provided via header/query/body
	if cs == nil {
		if b64, ok := apiutil.ExtractKubeconfigB64(c); ok {
			if _, clientset, err := apiutil.InitClientsFromKubeconfigB64(c, b64); err == nil {
				cs = clientset
			}
		} else {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "kubeconfig not provided or invalid"})
			return
		}
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

	resp := gin.H{
		"message":          "connection successful",
		"user":             user,
		"context":          ctxName,
		"defaultNamespace": defNs,
	}

	// If we have a real clientset, try a lightweight connectivity check; otherwise skip
	if cs != nil {
		// 1) 与 apiserver 通信
		sv, err := cs.Discovery().ServerVersion()
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "apiserver unreachable", "details": err.Error()})
			return
		}
		// 2) RBAC 轻量校验：列出命名空间（限制 1）
		if _, err := cs.CoreV1().Namespaces().List(c.Request.Context(), metav1.ListOptions{Limit: 1}); err != nil {
			if statusErr, ok := err.(k8serrors.APIStatus); ok {
				code := int(statusErr.Status().Code)
				if code == 0 {
					code = http.StatusInternalServerError
				}
				c.JSON(code, gin.H{"error": "failed to list namespaces", "details": err.Error()})
			} else {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list namespaces", "details": err.Error()})
			}
			return
		}
		resp["apiserverVersion"] = sv.GitVersion
		resp["platform"] = sv.Platform
	}

	c.JSON(http.StatusOK, resp)
}

// Note: error handling and client getters are centralized in api/util.
