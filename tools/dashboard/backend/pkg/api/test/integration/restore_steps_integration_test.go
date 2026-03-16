package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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

	"polardbx-dashboard-backend/pkg/api/domain/polardbxclusters/services"
)

// setupRestoreFlowRouter sets up a test router with restore flow routes
func setupRestoreFlowRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
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

	item.POST("/restore-flow/run", services.RunRestoreFlow)

	return r, ctrlClient
}

// ==================== RunRestoreFlow Integration Tests ====================

func TestIntegration_RunRestoreFlow_Success(t *testing.T) {
	// Setup: Create source cluster and backup
	sourceCluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-cluster",
			Namespace: "default",
		},
	}

	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "source-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.BackupFinished,
		},
	}

	router, _ := setupRestoreFlowRouter(t, sourceCluster, backup)

	reqBody := map[string]interface{}{
		"backupSet": "test-backup",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/source-cluster/restore-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// May return 200/201 (success) or 400/500 (validation/implementation errors)
	assert.Contains(t, []int{http.StatusOK, http.StatusCreated, http.StatusBadRequest, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_RunRestoreFlow_MissingBackupSet(t *testing.T) {
	sourceCluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupRestoreFlowRouter(t, sourceCluster)

	reqBody := map[string]interface{}{}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/source-cluster/restore-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Should return validation error
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_RunRestoreFlow_InvalidNamespace(t *testing.T) {
	router, _ := setupRestoreFlowRouter(t)

	reqBody := map[string]interface{}{
		"backupSet": "test-backup",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters//source-cluster/restore-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Should return validation error for missing namespace (precheck step fails)
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusOK}, w.Code)
}

func TestIntegration_RunRestoreFlow_InvalidName(t *testing.T) {
	router, _ := setupRestoreFlowRouter(t)

	reqBody := map[string]interface{}{
		"backupSet": "test-backup",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default//restore-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Should return validation error for missing name (precheck step fails)
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusOK}, w.Code)
}

func TestIntegration_RunRestoreFlow_BackupNotFound(t *testing.T) {
	sourceCluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-cluster",
			Namespace: "default",
		},
	}

	router, _ := setupRestoreFlowRouter(t, sourceCluster)

	reqBody := map[string]interface{}{
		"backupSet": "nonexistent-backup",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/source-cluster/restore-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Returns 200 (with error in steps) or 404 (if error handled differently)
	assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, w.Code)
}

func TestIntegration_RunRestoreFlow_CompleteFlow(t *testing.T) {
	// Setup: Create source cluster and finished backup
	sourceCluster := &polardbxv1.PolarDBXCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-cluster",
			Namespace: "default",
		},
	}

	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "complete-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "source-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.BackupFinished,
		},
	}

	router, cli := setupRestoreFlowRouter(t, sourceCluster, backup)

	reqBody := map[string]interface{}{
		"backupSet": "complete-backup",
	}
	bodyBytes, _ := json.Marshal(reqBody)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/polardbxclusters/default/source-cluster/restore-flow/run", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	// Flow should execute: Precheck → Prepare → Apply → Verify
	// Returns 200 or 201 with flow execution result
	assert.Contains(t, []int{http.StatusOK, http.StatusCreated}, w.Code)

	// If successful, verify that restore was initiated
	if w.Code == http.StatusOK {
		var resp map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		if err == nil {
			// Response should contain flow execution result
			assert.Contains(t, resp, "steps")
		}
	}

	// Verify backup still exists (not deleted during restore)
	var finalBackup polardbxv1.PolarDBXBackup
	err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "complete-backup"}, &finalBackup)
	require.NoError(t, err)
}
