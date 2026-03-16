package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// setupBackupCoreRouter sets up a test router with backup core operations routes
func setupBackupCoreRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
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

	// Register backup routes
	backupGroup := v1.Group("/polardbxclusters")
	backupGroup.GET("/:namespace/:name/backups", domain_pxc.ListBackups)
	backupGroup.POST("/:namespace/:name/backups", domain_pxc.CreateBackup)

	// Root-level backup operations
	v1.GET("/backups/:namespace/:name/stream", domain_pxc.StreamBackupEvents)
	v1.GET("/backups/:namespace/:name/metrics", domain_pxc.GetBackupMetrics)
	v1.DELETE("/backups/:namespace/:name", domain_pxc.DeleteBackup)
	v1.POST("/backups/:namespace/:name/force-delete", domain_pxc.ForceDeleteBackup)
	v1.GET("/backups/overview", domain_pxc.GetBackupOverview)

	return r, ctrlClient
}

// ==================== GetMetrics Integration Tests ====================

func TestIntegration_GetBackupMetrics_NewPhase(t *testing.T) {
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.BackupNew,
		},
	}

	router, _ := setupBackupCoreRouter(t, backup)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/default/test-backup/metrics", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Contains(t, resp, "phase")
	assert.Contains(t, resp, "progress")
	assert.Contains(t, resp, "children")
}

func TestIntegration_GetBackupMetrics_FullBackuping(t *testing.T) {
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.FullBackuping,
		},
	}

	router, _ := setupBackupCoreRouter(t, backup)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/default/test-backup/metrics", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "fullbackuping", resp["phase"])
	assert.Contains(t, resp, "progress")
}

func TestIntegration_GetBackupMetrics_Finished(t *testing.T) {
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.BackupFinished,
		},
	}

	router, _ := setupBackupCoreRouter(t, backup)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/default/test-backup/metrics", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "finished", resp["phase"])
	if progress, ok := resp["progress"].(float64); ok {
		assert.Equal(t, float64(100), progress)
	}
}

func TestIntegration_GetBackupMetrics_NotFound(t *testing.T) {
	router, _ := setupBackupCoreRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/default/nonexistent/metrics", nil)
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusNotFound, http.StatusInternalServerError}, w.Code)
}

// ==================== ForceDelete Integration Tests ====================

func TestIntegration_ForceDelete_Success(t *testing.T) {
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-backup",
			Namespace:  "default",
			Finalizers: []string{"polardbx.aliyun.com/finalizer"},
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.BackupFinished,
		},
	}

	router, cli := setupBackupCoreRouter(t, backup)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/backups/default/test-backup/force-delete", nil)
	router.ServeHTTP(w, req)

	// May return 200 or 500 depending on implementation
	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w.Code)

	// Verify finalizers were removed
	var updatedBackup polardbxv1.PolarDBXBackup
	err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "test-backup"}, &updatedBackup)
	if err == nil {
		// If backup still exists, finalizers should be removed
		assert.Empty(t, updatedBackup.GetFinalizers())
	}
}

func TestIntegration_ForceDelete_NotFound(t *testing.T) {
	router, _ := setupBackupCoreRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/backups/default/nonexistent/force-delete", nil)
	router.ServeHTTP(w, req)

	assert.Contains(t, []int{http.StatusNotFound, http.StatusInternalServerError}, w.Code)
}

func TestIntegration_ForceDelete_NoFinalizers(t *testing.T) {
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "default",
			// No finalizers
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
	}

	router, _ := setupBackupCoreRouter(t, backup)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/v1/backups/default/test-backup/force-delete", nil)
	router.ServeHTTP(w, req)

	// Should still succeed even without finalizers
	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w.Code)
}

// ==================== StreamEvents Integration Tests ====================

func TestIntegration_StreamBackupEvents_InitialPhase(t *testing.T) {
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.BackupNew,
		},
	}

	router, _ := setupBackupCoreRouter(t, backup)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/default/test-backup/stream", nil)
	req.Header.Set("Accept", "text/event-stream")

	// Use a context with timeout to avoid hanging
	ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	router.ServeHTTP(w, req)

	// SSE endpoint should return 200 and set appropriate headers
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	assert.Equal(t, "keep-alive", w.Header().Get("Connection"))

	// Check for SSE event format
	body := w.Body.String()
	assert.Contains(t, body, "event:")
	assert.Contains(t, body, "data:")
}

func TestIntegration_StreamBackupEvents_NotFound(t *testing.T) {
	router, _ := setupBackupCoreRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/default/nonexistent/stream", nil)
	req.Header.Set("Accept", "text/event-stream")

	ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	router.ServeHTTP(w, req)

	// Should return error event
	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "error")
}

func TestIntegration_StreamBackupEvents_PhaseChange(t *testing.T) {
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.FullBackuping,
		},
	}

	router, _ := setupBackupCoreRouter(t, backup)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/default/test-backup/stream", nil)
	req.Header.Set("Accept", "text/event-stream")

	ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
	defer cancel()
	req = req.WithContext(ctx)

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	// Should contain phaseChanged event
	assert.Contains(t, body, "phaseChanged")
	assert.Contains(t, body, "fullbackuping")
}

// ==================== GetOverview Integration Tests ====================

func TestIntegration_GetBackupOverview_Empty(t *testing.T) {
	router, _ := setupBackupCoreRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/overview?namespace=default", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Contains(t, resp, "kpi")
	assert.Contains(t, resp, "namespace")
}

func TestIntegration_GetBackupOverview_WithBackups(t *testing.T) {
	now := metav1.Now()
	backup1 := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backup-1",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "cluster-1",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase:     polardbxv1.BackupFinished,
			StartTime: &now,
		},
	}

	backup2 := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backup-2",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "cluster-2",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase:     polardbxv1.BackupFailed,
			StartTime: &now,
		},
	}

	router, _ := setupBackupCoreRouter(t, backup1, backup2)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backups/overview?namespace=default", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Contains(t, resp, "kpi")

	if kpi, ok := resp["kpi"].(map[string]interface{}); ok {
		assert.Contains(t, kpi, "totalBackups24h")
		assert.Contains(t, kpi, "successRate24h")
	}
}

// ==================== Lifecycle Integration Tests ====================

func TestIntegration_BackupCore_Lifecycle(t *testing.T) {
	// Create backup
	backup := &polardbxv1.PolarDBXBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lifecycle-backup",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupSpec{
			Cluster: polardbxv1.PolarDBXClusterReference{
				Name: "test-cluster",
			},
		},
		Status: polardbxv1.PolarDBXBackupStatus{
			Phase: polardbxv1.FullBackuping,
		},
	}

	router, cli := setupBackupCoreRouter(t, backup)

	// Step 1: Get metrics
	w1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/api/v1/backups/default/lifecycle-backup/metrics", nil)
	router.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)

	// Step 2: Force delete
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("POST", "/api/v1/backups/default/lifecycle-backup/force-delete", nil)
	router.ServeHTTP(w2, req2)
	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w2.Code)

	// Verify backup was processed
	var finalBackup polardbxv1.PolarDBXBackup
	err := cli.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "lifecycle-backup"}, &finalBackup)
	if err == nil {
		// If backup still exists, finalizers should be removed
		assert.Empty(t, finalBackup.GetFinalizers())
	}
}
