package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api_restore "polardbx-ui-backend/pkg/api/restore"
)

func setupRecoveryTestRouter(fakeClient client.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
	})

	// Register recovery routes (subpackage)
	router.POST("/clusters/:namespace/:name/restore", api_restore.RestoreCluster)
	router.POST("/clusters/:namespace/:name/pitr", api_restore.InitiatePITR)
	router.GET("/clusters/:namespace/:name/restore-status", api_restore.GetRestoreStatus)
	router.GET("/restore-jobs", api_restore.ListJobs)
	router.GET("/restore-jobs/:namespace/:name", api_restore.GetJob)
	router.DELETE("/restore-jobs/:namespace/:name", api_restore.CancelJob)

	return router
}

func TestRecoveryEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)

	// --- Test Cases ---
	t.Run("RestoreCluster_Success", func(t *testing.T) {
		// prepare required objects: finished backup and source cluster
		backup := &polardbxv1.PolarDBXBackup{
			ObjectMeta: metav1.ObjectMeta{Name: "my-backup-set", Namespace: "default"},
			Status:     polardbxv1.PolarDBXBackupStatus{Phase: polardbxv1.BackupFinished},
		}
		source := &polardbxv1.PolarDBXCluster{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"}}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(backup, source).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		restoreReq := gin.H{
			"backupName": "my-backup-set",
			"targetName": "restored-cluster",
			"storageProvider": gin.H{
				"type":   "oss",
				"config": gin.H{},
			},
		}
		body, _ := json.Marshal(restoreReq)
		req, _ := http.NewRequest(http.MethodPost, "/clusters/default/test-cluster/restore", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
	})

	t.Run("RestoreCluster_InvalidRequest", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		// Missing required backupName field
		restoreReq := gin.H{
			"targetName": "restored-cluster",
		}
		body, _ := json.Marshal(restoreReq)
		req, _ := http.NewRequest(http.MethodPost, "/clusters/default/test-cluster/restore", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("InitiatePITR_Success", func(t *testing.T) {
		source := &polardbxv1.PolarDBXCluster{ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"}}
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(source).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		pitrReq := gin.H{
			"time":       "2023-12-01T10:00:00Z",
			"targetName": "pitr-cluster",
		}
		body, _ := json.Marshal(pitrReq)
		req, _ := http.NewRequest(http.MethodPost, "/clusters/default/test-cluster/pitr", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
	})

	t.Run("InitiatePITR_InvalidRequest", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		// Missing required time/targetTime field
		pitrReq := gin.H{
			"targetName": "pitr-cluster",
		}
		body, _ := json.Marshal(pitrReq)
		req, _ := http.NewRequest(http.MethodPost, "/clusters/default/test-cluster/pitr", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("GetRestoreStatus_NotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/clusters/default/restoring-cluster/restore-status", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("ListRestoreJobs", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/restore-jobs?namespace=default", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("GetRestoreJob_NotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/restore-jobs/default/test-job", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("CancelRestoreJob_NotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupRecoveryTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodDelete, "/restore-jobs/default/test-job", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}
