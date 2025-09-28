package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_xs "polardbx-ui-backend/pkg/api/domain/xstores"
)

func setupXStoreBackupTestRouter(fakeClient client.Client) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
	})

	// Register XStoreBackup routes (domain)
	router.GET("/xstore-backups", domain_xs.ListBackups)
	router.POST("/xstore-backups", domain_xs.CreateBackup)
	router.GET("/xstore-backups/:namespace/:name", domain_xs.GetBackup)
	router.PUT("/xstore-backups/:namespace/:name", domain_xs.UpdateBackup)
	router.DELETE("/xstore-backups/:namespace/:name", domain_xs.DeleteBackup)

	return router
}

func TestXStoreBackupEndpoints(t *testing.T) {
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)

	// --- Test Data ---
	sampleBackup := &polardbxv1.XStoreBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "xstore-backup-1",
			Namespace: "default",
		},
		Spec: polardbxv1.XStoreBackupSpec{
			Engine: "galaxy",
			XStore: polardbxv1.XStoreReference{
				Name: "xstore1",
			},
			StorageProvider: polardbx.BackupStorageProvider{
				StorageName: polardbx.OSS,
				Sink:        "oss://bucket/path",
			},
			PreferredBackupRole: "follower",
			CleanPolicy:         polardbx.CleanPolicyRetain,
		},
	}

	// --- Test Cases ---
	t.Run("CreateXStoreBackup", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		newBackup := sampleBackup.DeepCopy()
		body, _ := json.Marshal(newBackup)
		req, _ := http.NewRequest(http.MethodPost, "/xstore-backups?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)
		var createdBackup polardbxv1.XStoreBackup
		err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(sampleBackup), &createdBackup)
		assert.NoError(t, err)
		assert.Equal(t, "xstore1", createdBackup.Spec.XStore.Name)
	})

	t.Run("ListXStoreBackups", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleBackup).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/xstore-backups?namespace=default", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var backups []polardbxv1.XStoreBackup
		err := json.Unmarshal(w.Body.Bytes(), &backups)
		assert.NoError(t, err)
		assert.Len(t, backups, 1)
		assert.Equal(t, "xstore-backup-1", backups[0].Name)
	})

	t.Run("GetXStoreBackup", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleBackup).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/xstore-backups/default/xstore-backup-1", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var backup polardbxv1.XStoreBackup
		err := json.Unmarshal(w.Body.Bytes(), &backup)
		assert.NoError(t, err)
		assert.Equal(t, "xstore-backup-1", backup.Name)
	})

	t.Run("UpdateXStoreBackup", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleBackup.DeepCopy()).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		updatedBackup := sampleBackup.DeepCopy()
		updatedBackup.Labels = map[string]string{"updated": "true"}
		body, _ := json.Marshal(updatedBackup)
		req, _ := http.NewRequest(http.MethodPut, "/xstore-backups/default/xstore-backup-1", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var backupInClient polardbxv1.XStoreBackup
		err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(sampleBackup), &backupInClient)
		assert.NoError(t, err)
		assert.Equal(t, "true", backupInClient.Labels["updated"])
	})

	t.Run("DeleteXStoreBackup", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleBackup.DeepCopy()).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodDelete, "/xstore-backups/default/xstore-backup-1", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		var deletedBackup polardbxv1.XStoreBackup
		err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(sampleBackup), &deletedBackup)
		assert.True(t, k8serrors.IsNotFound(err))
	})

	t.Run("CreateXStoreBackup_InvalidJSON", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodPost, "/xstore-backups?namespace=default", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")

		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("GetXStoreBackup_NotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodGet, "/xstore-backups/default/non-existent", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("DeleteXStoreBackup_NotFound", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		router := setupXStoreBackupTestRouter(fakeClient)
		w := httptest.NewRecorder()

		req, _ := http.NewRequest(http.MethodDelete, "/xstore-backups/default/non-existent", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}
