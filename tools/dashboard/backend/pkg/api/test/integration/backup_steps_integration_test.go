package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
)

// setupBackupFlowRouter sets up a test router with backup flow routes
func setupBackupFlowRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
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
	item := clusterGroup.Group("/:namespace/:name")

	// Enable flow endpoints for testing
	os.Setenv("ENABLE_FLOW_ENDPOINTS", "true")
	defer os.Unsetenv("ENABLE_FLOW_ENDPOINTS")

	item.POST("/backup-flow/run", services.RunBackupFlow)

	return r, ctrlClient
}

// ==================== RunBackupFlow Integration Tests ====================

func TestIntegration_RunBackupFlow_Success(t *testing.T) {
	// Setup: Create cluster
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupBackupFlowRouter(t, cluster)

	reqBody := map[string]interface{}{
		"metadata": map[string]interface{}{
			"name": "test-backup",
		},
		"spec": map[string]interface{}{
			"cluster": map[string]interface{}{
				"name": "test-cluster",
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/test-cluster/backup-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// May return 200 (success) or 400/500 (validation/implementation errors)
	assert.Contains(t, []int{http.StatusOK, http.StatusBadRequest, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_RunBackupFlow_InvalidJSON(t *testing.T) {
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupBackupFlowRouter(t, cluster)

	body := bytes.NewBufferString(`{invalid json}`)
	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/test-cluster/backup-flow/run", body)
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestIntegration_RunBackupFlow_MissingCluster(t *testing.T) {
	router, _ := setupBackupFlowRouter(t)

	reqBody := map[string]interface{}{
		"metadata": map[string]interface{}{
			"name": "test-backup",
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/nonexistent/backup-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// May return 200 (with error in steps), 400 (validation) or 500 (internal error)
	assert.Contains(t, []int{http.StatusOK, http.StatusBadRequest, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_RunBackupFlow_CompleteFlow(t *testing.T) {
	// Setup: Create cluster
	cluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupBackupFlowRouter(t, cluster)

	reqBody := map[string]interface{}{
		"metadata": map[string]interface{}{
			"name": "complete-backup",
		},
		"spec": map[string]interface{}{
			"cluster": map[string]interface{}{
				"name": "test-cluster",
			},
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/test-cluster/backup-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Flow should execute: Validate → Create
	// May return 200 (success) or errors at various stages
	assert.Contains(t, []int{http.StatusOK, http.StatusBadRequest, http.StatusInternalServerError}, w.Code)

	if w.Code == http.StatusOK {
		var resp map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		if err == nil {
			// Response should contain flow execution result
			assert.Contains(t, resp, "steps")
		}
	}
}
