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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
)

func setupBackupBinlogTest() (*gin.Engine, client.Client) {
	gin.SetMode(gin.TestMode)

	// Create scheme with all required types
	scheme := runtime.NewScheme()
	polardbxv1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)

	// Create fake client
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Create gin router and register routes
	router := gin.New()

	// Add middleware to inject client
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		c.Next()
	})

	// Register BackupBinlog routes
	RegisterBackupBinlogRoutes(router)

	return router, fakeClient
}

func TestBackupBinlogEndpoints(t *testing.T) {
	router, fakeClient := setupBackupBinlogTest()

	// Create test BackupBinlog
	testBackupBinlog := &polardbxv1.PolarDBXBackupBinlog{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup-binlog",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupBinlogSpec{
			PxcName:              "test-cluster",
			PxcUid:               "test-uid-12345",
			RemoteExpireLogHours: intstr.FromInt(168), // 7 days
			LocalExpireLogHours:  intstr.FromInt(7),   // 7 hours
			MaxLocalBinlogCount:  60,
			PointInTimeRecover:   true,
			BinlogChecksum:       "CRC32",
			StorageProvider: polardbx.BackupStorageProvider{
				StorageName: polardbx.OSS,
				Sink:        "oss://bucket/binlog/",
			},
		},
	}

	// Create the binlog in fake client for tests that need it
	err := fakeClient.Create(context.TODO(), testBackupBinlog)
	assert.NoError(t, err)

	t.Run("ListBackupBinlogs", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/backup-binlogs?namespace=default", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var binlogs []polardbxv1.PolarDBXBackupBinlog
		err := json.Unmarshal(resp.Body.Bytes(), &binlogs)
		assert.NoError(t, err)
		assert.Len(t, binlogs, 1)
		assert.Equal(t, "test-backup-binlog", binlogs[0].Name)
		assert.Equal(t, "test-cluster", binlogs[0].Spec.PxcName)
	})

	t.Run("GetBackupBinlog", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/backup-binlogs/default/test-backup-binlog", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var binlog polardbxv1.PolarDBXBackupBinlog
		err := json.Unmarshal(resp.Body.Bytes(), &binlog)
		assert.NoError(t, err)
		assert.Equal(t, "test-backup-binlog", binlog.Name)
		assert.Equal(t, "test-cluster", binlog.Spec.PxcName)
		assert.Equal(t, "test-uid-12345", binlog.Spec.PxcUid)
		assert.Equal(t, uint64(60), binlog.Spec.MaxLocalBinlogCount)
		assert.Equal(t, true, binlog.Spec.PointInTimeRecover)
		assert.Equal(t, "CRC32", binlog.Spec.BinlogChecksum)
		assert.Equal(t, polardbx.OSS, binlog.Spec.StorageProvider.StorageName)
	})

	t.Run("CreateBackupBinlog", func(t *testing.T) {
		newBinlog := polardbxv1.PolarDBXBackupBinlog{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-backup-binlog",
			},
			Spec: polardbxv1.PolarDBXBackupBinlogSpec{
				PxcName:              "new-test-cluster",
				RemoteExpireLogHours: intstr.FromInt(240), // 10 days
				LocalExpireLogHours:  intstr.FromInt(12),  // 12 hours
				MaxLocalBinlogCount:  100,
				PointInTimeRecover:   false,
				BinlogChecksum:       "MD5",
				StorageProvider: polardbx.BackupStorageProvider{
					StorageName: polardbx.MINIO,
					Sink:        "s3://bucket/binlog/",
				},
			},
		}

		jsonData, _ := json.Marshal(newBinlog)
		req, _ := http.NewRequest("POST", "/api/v1/backup-binlogs?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdBinlog polardbxv1.PolarDBXBackupBinlog
		err := json.Unmarshal(resp.Body.Bytes(), &createdBinlog)
		assert.NoError(t, err)
		assert.Equal(t, "new-backup-binlog", createdBinlog.Name)
		assert.Equal(t, "default", createdBinlog.Namespace)
		assert.Equal(t, "new-test-cluster", createdBinlog.Spec.PxcName)
		assert.Equal(t, uint64(100), createdBinlog.Spec.MaxLocalBinlogCount)
		assert.Equal(t, false, createdBinlog.Spec.PointInTimeRecover)
		assert.Equal(t, polardbx.MINIO, createdBinlog.Spec.StorageProvider.StorageName)
	})

	t.Run("UpdateBackupBinlog", func(t *testing.T) {
		updatedBinlog := *testBackupBinlog
		updatedBinlog.Spec.MaxLocalBinlogCount = 120
		updatedBinlog.Spec.PointInTimeRecover = false
		updatedBinlog.Spec.RemoteExpireLogHours = intstr.FromInt(336) // 14 days

		jsonData, _ := json.Marshal(updatedBinlog)
		req, _ := http.NewRequest("PUT", "/api/v1/backup-binlogs/default/test-backup-binlog", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		var returnedBinlog polardbxv1.PolarDBXBackupBinlog
		err := json.Unmarshal(resp.Body.Bytes(), &returnedBinlog)
		assert.NoError(t, err)
		assert.Equal(t, uint64(120), returnedBinlog.Spec.MaxLocalBinlogCount)
		assert.Equal(t, false, returnedBinlog.Spec.PointInTimeRecover)
		assert.Equal(t, intstr.FromInt(336), returnedBinlog.Spec.RemoteExpireLogHours)
	})

	t.Run("DeleteBackupBinlog", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", "/api/v1/backup-binlogs/default/test-backup-binlog", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusOK, resp.Code)

		// Verify the binlog is deleted
		var binlog polardbxv1.PolarDBXBackupBinlog
		err := fakeClient.Get(context.TODO(), client.ObjectKey{
			Namespace: "default",
			Name:      "test-backup-binlog",
		}, &binlog)
		assert.Error(t, err) // Should be not found error
	})

	t.Run("CreateBackupBinlogInvalidJSON", func(t *testing.T) {
		req, _ := http.NewRequest("POST", "/api/v1/backup-binlogs?namespace=default", bytes.NewBuffer([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("GetNonExistentBackupBinlog", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/api/v1/backup-binlogs/default/non-existent-binlog", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusNotFound, resp.Code)
	})

	t.Run("DeleteNonExistentBackupBinlog", func(t *testing.T) {
		req, _ := http.NewRequest("DELETE", "/api/v1/backup-binlogs/default/non-existent-binlog", nil)
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusNotFound, resp.Code)
	})
}

func TestBackupBinlogBusinessLogic(t *testing.T) {
	router, _ := setupBackupBinlogTest()

	t.Run("TestDifferentStorageProviders", func(t *testing.T) {
		testCases := []struct {
			name        string
			binlog      polardbxv1.PolarDBXBackupBinlog
			description string
		}{
			{
				name: "OSSStorage",
				binlog: polardbxv1.PolarDBXBackupBinlog{
					ObjectMeta: metav1.ObjectMeta{
						Name: "oss-backup-binlog",
					},
					Spec: polardbxv1.PolarDBXBackupBinlogSpec{
						PxcName: "oss-cluster",
						StorageProvider: polardbx.BackupStorageProvider{
							StorageName: polardbx.OSS,
							Sink:        "oss://my-bucket/binlogs/",
						},
						PointInTimeRecover: true,
					},
				},
				description: "Backup binlog with Alibaba Cloud OSS storage",
			},
			{
				name: "S3Storage",
				binlog: polardbxv1.PolarDBXBackupBinlog{
					ObjectMeta: metav1.ObjectMeta{
						Name: "s3-backup-binlog",
					},
					Spec: polardbxv1.PolarDBXBackupBinlogSpec{
						PxcName: "s3-cluster",
						StorageProvider: polardbx.BackupStorageProvider{
							StorageName: polardbx.MINIO,
							Sink:        "s3://my-bucket/binlogs/",
						},
						PointInTimeRecover: true,
					},
				},
				description: "Backup binlog with S3-compatible storage",
			},
			{
				name: "SFTPStorage",
				binlog: polardbxv1.PolarDBXBackupBinlog{
					ObjectMeta: metav1.ObjectMeta{
						Name: "sftp-backup-binlog",
					},
					Spec: polardbxv1.PolarDBXBackupBinlogSpec{
						PxcName: "sftp-cluster",
						StorageProvider: polardbx.BackupStorageProvider{
							StorageName: polardbx.SFTP,
							Sink:        "sftp://server:22/backups/binlogs/",
						},
						PointInTimeRecover: false,
					},
				},
				description: "Backup binlog with SFTP storage",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				jsonData, _ := json.Marshal(tc.binlog)
				req, _ := http.NewRequest("POST", "/api/v1/backup-binlogs?namespace=default", bytes.NewBuffer(jsonData))
				req.Header.Set("Content-Type", "application/json")
				resp := httptest.NewRecorder()

				router.ServeHTTP(resp, req)

				assert.Equal(t, http.StatusCreated, resp.Code)

				var createdBinlog polardbxv1.PolarDBXBackupBinlog
				err := json.Unmarshal(resp.Body.Bytes(), &createdBinlog)
				assert.NoError(t, err)
				assert.Equal(t, tc.binlog.Name, createdBinlog.Name)
				assert.Equal(t, tc.binlog.Spec.PxcName, createdBinlog.Spec.PxcName)
				assert.Equal(t, tc.binlog.Spec.StorageProvider.StorageName, createdBinlog.Spec.StorageProvider.StorageName)
				assert.Equal(t, tc.binlog.Spec.PointInTimeRecover, createdBinlog.Spec.PointInTimeRecover)
			})
		}
	})

	t.Run("TestRetentionPolicies", func(t *testing.T) {
		// Test with different retention policies
		retentionBinlog := polardbxv1.PolarDBXBackupBinlog{
			ObjectMeta: metav1.ObjectMeta{
				Name: "retention-test-binlog",
			},
			Spec: polardbxv1.PolarDBXBackupBinlogSpec{
				PxcName:              "retention-cluster",
				RemoteExpireLogHours: intstr.FromInt(720), // 30 days
				LocalExpireLogHours:  intstr.FromInt(24),  // 1 day
				MaxLocalBinlogCount:  200,
				PointInTimeRecover:   true,
				BinlogChecksum:       "SHA256",
				StorageProvider: polardbx.BackupStorageProvider{
					StorageName: polardbx.OSS,
					Sink:        "oss://long-term-bucket/binlogs/",
				},
			},
		}

		jsonData, _ := json.Marshal(retentionBinlog)
		req, _ := http.NewRequest("POST", "/api/v1/backup-binlogs?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdBinlog polardbxv1.PolarDBXBackupBinlog
		err := json.Unmarshal(resp.Body.Bytes(), &createdBinlog)
		assert.NoError(t, err)
		assert.Equal(t, "retention-test-binlog", createdBinlog.Name)
		assert.Equal(t, intstr.FromInt(720), createdBinlog.Spec.RemoteExpireLogHours)
		assert.Equal(t, intstr.FromInt(24), createdBinlog.Spec.LocalExpireLogHours)
		assert.Equal(t, uint64(200), createdBinlog.Spec.MaxLocalBinlogCount)
		assert.Equal(t, "SHA256", createdBinlog.Spec.BinlogChecksum)
	})

	t.Run("TestPITRConfiguration", func(t *testing.T) {
		// Test Point-in-Time Recovery configuration
		pitrBinlog := polardbxv1.PolarDBXBackupBinlog{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pitr-test-binlog",
			},
			Spec: polardbxv1.PolarDBXBackupBinlogSpec{
				PxcName:              "pitr-cluster",
				PxcUid:               "pitr-uid-98765",
				PointInTimeRecover:   true,
				RemoteExpireLogHours: intstr.FromInt(8760), // 1 year
				StorageProvider: polardbx.BackupStorageProvider{
					StorageName: polardbx.MINIO,
					Sink:        "s3://pitr-bucket/binlogs/",
				},
			},
		}

		jsonData, _ := json.Marshal(pitrBinlog)
		req, _ := http.NewRequest("POST", "/api/v1/backup-binlogs?namespace=default", bytes.NewBuffer(jsonData))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()

		router.ServeHTTP(resp, req)

		assert.Equal(t, http.StatusCreated, resp.Code)

		var createdBinlog polardbxv1.PolarDBXBackupBinlog
		err := json.Unmarshal(resp.Body.Bytes(), &createdBinlog)
		assert.NoError(t, err)
		assert.Equal(t, "pitr-test-binlog", createdBinlog.Name)
		assert.Equal(t, "pitr-cluster", createdBinlog.Spec.PxcName)
		assert.Equal(t, "pitr-uid-98765", createdBinlog.Spec.PxcUid)
		assert.Equal(t, true, createdBinlog.Spec.PointInTimeRecover)
		assert.Equal(t, intstr.FromInt(8760), createdBinlog.Spec.RemoteExpireLogHours)
	})
}

// RegisterBackupBinlogRoutes registers the BackupBinlog related routes
func RegisterBackupBinlogRoutes(router *gin.Engine) {
	api := router.Group("/api/v1")
	{
		api.GET("/backup-binlogs", domain_pxc.ListBackupBinlogs)
		api.POST("/backup-binlogs", domain_pxc.CreateBackupBinlog)
		api.GET("/backup-binlogs/:namespace/:name", domain_pxc.GetBackupBinlog)
		api.PUT("/backup-binlogs/:namespace/:name", domain_pxc.UpdateBackupBinlog)
		api.DELETE("/backup-binlogs/:namespace/:name", domain_pxc.DeleteBackupBinlog)
	}
}
