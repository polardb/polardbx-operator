package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	polardbx "github.com/alibaba/polardbx-operator/api/v1/polardbx"
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

// Helper function to create int32 pointer
func int32Ptr(i int32) *int32 {
	return &i
}

// setupClusterOpsRouter sets up a test router with cluster operations routes
func setupClusterOpsRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
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
	item := clusterGroup.Group("/:namespace/:name")
	item.GET("", domain_pxc.Get)
	item.PUT("", domain_pxc.Update)
	item.DELETE("", domain_pxc.Delete)
	item.PATCH("/log-config/:nodeType", domain_pxc.UpdateLogConfig)
	item.PATCH("/scale", domain_pxc.Scale)
	item.PATCH("/upgrade", domain_pxc.Upgrade)

	return r, ctrlClient
}

// ==================== UpdateLogConfig Integration Tests ====================

func TestIntegration_UpdateLogConfig_CN_Success(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Nodes: polardbx.TopologyNodes{
					CN: polardbx.TopologyNodeCN{
						Replicas: int32Ptr(1),
					},
				},
			},
		},
	}

	router, cli := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"logLevel":         "INFO",
		"enableAuditLog":   true,
		"slowLogThreshold": 1000,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/log-config/cn", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify the cluster was updated
	var updatedCluster polardbxv1.PolarDBXCluster
	err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "test-cluster"}, &updatedCluster)
	require.NoError(t, err)
	assert.NotNil(t, updatedCluster.Spec.Config)
	assert.NotNil(t, updatedCluster.Spec.Config.CN)
}

func TestIntegration_UpdateLogConfig_DN_Success(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Nodes: polardbx.TopologyNodes{
					DN: polardbx.TopologyNodeDN{
						Replicas: 1,
					},
				},
			},
		},
	}

	router, _ := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"logLevel":       "DEBUG",
		"enableAuditLog": false,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/log-config/dn", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestIntegration_UpdateLogConfig_InvalidNodeType(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"logLevel": "INFO",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/log-config/invalid", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIntegration_UpdateLogConfig_NotFound(t *testing.T) {
	router, _ := setupClusterOpsRouter(t)

	reqBody := map[string]interface{}{
		"logLevel": "INFO",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/nonexistent/log-config/cn", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// May return 404 or 500 depending on implementation
	assert.Contains(t, []int{http.StatusNotFound, http.StatusInternalServerError}, w.Code)
}

// ==================== Scale Integration Tests ====================

func TestIntegration_Scale_CN_Success(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Nodes: polardbx.TopologyNodes{
					CN: polardbx.TopologyNodeCN{
						Replicas: int32Ptr(1),
					},
				},
			},
		},
	}

	router, cli := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"cnReplicas": 3,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/scale", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify the cluster was updated
	var updatedCluster polardbxv1.PolarDBXCluster
	err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "test-cluster"}, &updatedCluster)
	require.NoError(t, err)
	if updatedCluster.Spec.Topology.Nodes.CN.Replicas != nil {
		assert.Equal(t, int32(3), *updatedCluster.Spec.Topology.Nodes.CN.Replicas)
	}
}

func TestIntegration_Scale_DN_Success(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Nodes: polardbx.TopologyNodes{
					DN: polardbx.TopologyNodeDN{
						Replicas: 1,
					},
				},
			},
		},
	}

	router, _ := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"dnReplicas": 2,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/scale", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestIntegration_Scale_MultipleNodes(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Nodes: polardbx.TopologyNodes{
					CN: polardbx.TopologyNodeCN{Replicas: int32Ptr(1)},
					DN: polardbx.TopologyNodeDN{Replicas: 1},
				},
			},
		},
	}

	router, _ := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"cnReplicas":  2,
		"dnReplicas":  3,
		"cdcReplicas": 1,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/scale", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestIntegration_Scale_NoReplicas(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/scale", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIntegration_Scale_NotFound(t *testing.T) {
	router, _ := setupClusterOpsRouter(t)

	reqBody := map[string]interface{}{
		"cnReplicas": 2,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/nonexistent/scale", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusNotFound, http.StatusInternalServerError}, w.Code)
}

// ==================== Upgrade Integration Tests ====================

func TestIntegration_Upgrade_Success(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Version: "5.4.17",
			},
		},
	}

	router, cli := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"targetVersion": "5.4.18",
		"strategy":      "RollingUpdate",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/upgrade", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Verify the cluster was updated
	var updatedCluster polardbxv1.PolarDBXCluster
	err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "test-cluster"}, &updatedCluster)
	require.NoError(t, err)
	assert.Equal(t, "5.4.18", updatedCluster.Spec.Topology.Version)
}

func TestIntegration_Upgrade_WithStrategy(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Version: "5.4.17",
			},
		},
	}

	router, _ := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"targetVersion": "5.4.19",
		"strategy":      "InPlace",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/upgrade", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestIntegration_Upgrade_MissingTargetVersion(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupClusterOpsRouter(t, cluster)

	reqBody := map[string]interface{}{
		"strategy": "RollingUpdate",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/test-cluster/upgrade", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Should return validation error
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIntegration_Upgrade_NotFound(t *testing.T) {
	router, _ := setupClusterOpsRouter(t)

	reqBody := map[string]interface{}{
		"targetVersion": "5.4.18",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/nonexistent/upgrade", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusNotFound, http.StatusInternalServerError}, w.Code)
}

// ==================== Lifecycle Integration Tests ====================

func TestIntegration_ClusterOps_Lifecycle(t *testing.T) {
	// Create cluster
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lifecycle-cluster",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXClusterSpec{
			Topology: polardbx.Topology{
				Version: "5.4.17",
				Nodes: polardbx.TopologyNodes{
					CN: polardbx.TopologyNodeCN{Replicas: int32Ptr(1)},
					DN: polardbx.TopologyNodeDN{Replicas: 1},
				},
			},
		},
	}

	router, cli := setupClusterOpsRouter(t, cluster)

	// Step 1: Update log config
	reqBody1 := map[string]interface{}{
		"logLevel": "DEBUG",
	}
	bodyBytes1, _ := json.Marshal(reqBody1)

	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/lifecycle-cluster/log-config/cn", bytes.NewBuffer(bodyBytes1))
	req1.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)

	// Step 2: Scale cluster
	reqBody2 := map[string]interface{}{
		"cnReplicas": 2,
	}
	bodyBytes2, _ := json.Marshal(reqBody2)

	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/lifecycle-cluster/scale", bytes.NewBuffer(bodyBytes2))
	req2.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)

	// Step 3: Upgrade cluster
	reqBody3 := map[string]interface{}{
		"targetVersion": "5.4.18",
	}
	bodyBytes3, _ := json.Marshal(reqBody3)

	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest("PATCH", "/api/v1/polardbxclusters/default/lifecycle-cluster/upgrade", bytes.NewBuffer(bodyBytes3))
	req3.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusOK, w3.Code)

	// Verify final state
	var finalCluster polardbxv1.PolarDBXCluster
	err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "lifecycle-cluster"}, &finalCluster)
	require.NoError(t, err)
	assert.Equal(t, "5.4.18", finalCluster.Spec.Topology.Version)
	if finalCluster.Spec.Topology.Nodes.CN.Replicas != nil {
		assert.Equal(t, int32(2), *finalCluster.Spec.Topology.Nodes.CN.Replicas)
	}
}
