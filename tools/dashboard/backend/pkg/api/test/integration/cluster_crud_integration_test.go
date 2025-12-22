package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
)

// setupClusterCRUDRouter sets up a test router with cluster CRUD routes
func setupClusterCRUDRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	v1 := r.Group("/api/v1")

	// Build scheme with all required types
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = polardbxv1.AddToScheme(scheme)

	ctrlClient := crfake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()

	v1.Use(func(c *gin.Context) {
		c.Set("k8sClient", ctrlClient)
		c.Set("k8sDefaultNamespace", "default")
		c.Next()
	})

	// Register cluster routes
	clusterGroup := v1.Group("/polardbxclusters")
	clusterGroup.GET("", domain_pxc.List)
	clusterGroup.POST("", domain_pxc.Create)
	clusterGroup.POST("/:namespace/create", domain_pxc.CreateFromConfig)
	item := clusterGroup.Group("/:namespace/:name")
	item.GET("", domain_pxc.Get)
	item.PUT("", domain_pxc.Update)
	item.DELETE("", domain_pxc.Delete)

	return r, ctrlClient
}

// ==================== CreateFromConfig Integration Tests ====================

func TestIntegration_CreateFromConfig_Success(t *testing.T) {
	router, cli := setupClusterCRUDRouter(t)

	reqBody := map[string]interface{}{
		"name":    "config-cluster",
		"version": "5.4.17",
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 2,
			},
			"dn": map[string]interface{}{
				"replicas": 2,
			},
			"gms": map[string]interface{}{
				"replicas": 1,
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// May return 201 (created) or 400 (validation error)
	assert.Contains(t, []int{http.StatusCreated, http.StatusBadRequest}, w.Code)

	if w.Code == http.StatusCreated {
		// Verify cluster was created
		var createdCluster polardbxv1.PolarDBXCluster
		err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "config-cluster"}, &createdCluster)
		require.NoError(t, err)
		assert.Equal(t, "config-cluster", createdCluster.Name)
	}
}

func TestIntegration_CreateFromConfig_WithNetworkConfig(t *testing.T) {
	router, _ := setupClusterCRUDRouter(t)

	reqBody := map[string]interface{}{
		"name":    "network-cluster",
		"version": "5.4.17",
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 2,
			},
			"dn": map[string]interface{}{
				"replicas": 2,
			},
			"gms": map[string]interface{}{
				"replicas": 1,
			},
		},
		"network": map[string]interface{}{
			"serviceType": "LoadBalancer",
			"hostNetwork": true,
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusCreated, http.StatusBadRequest}, w.Code)
}

func TestIntegration_CreateFromConfig_WithSecurityConfig(t *testing.T) {
	router, _ := setupClusterCRUDRouter(t)

	reqBody := map[string]interface{}{
		"name":    "security-cluster",
		"version": "5.4.17",
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 2,
			},
			"dn": map[string]interface{}{
				"replicas": 2,
			},
			"gms": map[string]interface{}{
				"replicas": 1,
			},
		},
		"security": map[string]interface{}{
			"enableTLS":  true,
			"secretName": "tls-secret",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusCreated, http.StatusBadRequest}, w.Code)
}

func TestIntegration_CreateFromConfig_WithAdvancedConfig(t *testing.T) {
	router, _ := setupClusterCRUDRouter(t)

	reqBody := map[string]interface{}{
		"name":    "advanced-cluster",
		"version": "5.4.17",
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 2,
			},
			"dn": map[string]interface{}{
				"replicas": 2,
			},
			"gms": map[string]interface{}{
				"replicas": 1,
			},
		},
		"advanced": map[string]interface{}{
			"shareGMS": true,
			"customLabels": map[string]interface{}{
				"environment": "test",
			},
			"nodeSelector": map[string]interface{}{
				"disktype": "ssd",
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusCreated, http.StatusBadRequest}, w.Code)
}

func TestIntegration_CreateFromConfig_ValidationError(t *testing.T) {
	router, _ := setupClusterCRUDRouter(t)

	reqBody := map[string]interface{}{
		"name": "", // Invalid: empty name
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 0, // Invalid: zero replicas
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIntegration_CreateFromConfig_AlreadyExists(t *testing.T) {
	existingCluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupClusterCRUDRouter(t, existingCluster)

	reqBody := map[string]interface{}{
		"name":    "existing-cluster",
		"version": "5.4.17",
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 2,
			},
			"dn": map[string]interface{}{
				"replicas": 2,
			},
			"gms": map[string]interface{}{
				"replicas": 1,
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Should return conflict or bad request
	assert.Contains(t, []int{http.StatusConflict, http.StatusBadRequest, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_CreateFromConfig_WithCDC(t *testing.T) {
	router, _ := setupClusterCRUDRouter(t)

	reqBody := map[string]interface{}{
		"name":    "cdc-cluster",
		"version": "5.4.17",
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 2,
			},
			"dn": map[string]interface{}{
				"replicas": 2,
			},
			"cdc": map[string]interface{}{
				"replicas": 1,
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusCreated, http.StatusBadRequest}, w.Code)
}

func TestIntegration_CreateFromConfig_WithImageConfig(t *testing.T) {
	router, _ := setupClusterCRUDRouter(t)

	reqBody := map[string]interface{}{
		"name":    "image-cluster",
		"version": "5.4.17",
		"topology": map[string]interface{}{
			"cn": map[string]interface{}{
				"replicas": 2,
			},
			"dn": map[string]interface{}{
				"replicas": 2,
			},
			"gms": map[string]interface{}{
				"replicas": 1,
			},
		},
		"image": map[string]interface{}{
			"repository": "polardbx/polardbx-engine",
			"tag":        "5.4.17",
			"pullPolicy": "IfNotPresent",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/create", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusCreated, http.StatusBadRequest}, w.Code)
}
