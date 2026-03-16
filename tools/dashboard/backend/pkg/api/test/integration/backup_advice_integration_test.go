package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
)

// setupBackupAdviceRouter sets up a test router with backup advice routes
func setupBackupAdviceRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
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

	// Register backup advice route
	clusterGroup := v1.Group("/polardbxclusters")
	item := clusterGroup.Group("/:namespace/:name")
	item.GET("/backup-advice", domain_pxc.GetBackupAdvice)

	return r, ctrlClient
}

// ==================== GetBackupAdvice Integration Tests ====================

func TestIntegration_GetBackupAdvice_WithFollowers(t *testing.T) {
	// Create XStores with followers (TotalPods > 1)
	xstore1 := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xstore-1",
			Namespace: "default",
			Labels: map[string]string{
				"polardbx/name": "test-cluster",
			},
		},
		Status: polardbxv1.XStoreStatus{
			TotalPods: 3, // Has followers
		},
	}

	xstore2 := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xstore-2",
			Namespace: "default",
			Labels: map[string]string{
				"polardbx/name": "test-cluster",
			},
		},
		Status: polardbxv1.XStoreStatus{
			TotalPods: 2, // Has followers
		},
	}

	router, _ := setupBackupAdviceRouter(t, xstore1, xstore2)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/backup-advice", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, true, resp["hasFollower"])
	assert.Equal(t, "follower", resp["role"])
}

func TestIntegration_GetBackupAdvice_WithoutFollowers(t *testing.T) {
	// Create XStores without followers (TotalPods <= 1)
	xstore1 := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xstore-1",
			Namespace: "default",
			Labels: map[string]string{
				"polardbx/name": "test-cluster",
			},
		},
		Status: polardbxv1.XStoreStatus{
			TotalPods: 1, // No followers
		},
	}

	xstore2 := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xstore-2",
			Namespace: "default",
			Labels: map[string]string{
				"polardbx/name": "test-cluster",
			},
		},
		Status: polardbxv1.XStoreStatus{
			TotalPods: 1, // No followers
		},
	}

	router, _ := setupBackupAdviceRouter(t, xstore1, xstore2)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/backup-advice", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, false, resp["hasFollower"])
	assert.Equal(t, "leader", resp["role"])
	assert.Contains(t, resp, "reason")
}

func TestIntegration_GetBackupAdvice_PartialFollowers(t *testing.T) {
	// Create XStores with mixed follower status
	xstore1 := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xstore-1",
			Namespace: "default",
			Labels: map[string]string{
				"polardbx/name": "test-cluster",
			},
		},
		Status: polardbxv1.XStoreStatus{
			TotalPods: 3, // Has followers
		},
	}

	xstore2 := &polardbxv1.XStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xstore-2",
			Namespace: "default",
			Labels: map[string]string{
				"polardbx/name": "test-cluster",
			},
		},
		Status: polardbxv1.XStoreStatus{
			TotalPods: 1, // No followers
		},
	}

	router, _ := setupBackupAdviceRouter(t, xstore1, xstore2)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/backup-advice", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, false, resp["hasFollower"])
	assert.Equal(t, "leader", resp["role"])
	assert.Contains(t, resp, "reason")
}

func TestIntegration_GetBackupAdvice_NoXStores(t *testing.T) {
	router, _ := setupBackupAdviceRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/test-cluster/backup-advice", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, false, resp["hasFollower"])
	assert.Equal(t, "leader", resp["role"])
	assert.Contains(t, resp, "reason")
	if reason, ok := resp["reason"].(string); ok {
		assert.Contains(t, reason, "no xstores")
	}
}

func TestIntegration_GetBackupAdvice_ClusterNotFound(t *testing.T) {
	router, _ := setupBackupAdviceRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/polardbxclusters/default/nonexistent/backup-advice", nil)
	router.ServeHTTP(w, req)

	// Should still return 200 with advice (no xstores found)
	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, false, resp["hasFollower"])
	assert.Equal(t, "leader", resp["role"])
}
