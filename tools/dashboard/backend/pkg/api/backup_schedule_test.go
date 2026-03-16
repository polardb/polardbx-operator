package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	polardbxv1 "github.com/alibaba/polardbx-operator/api/v1"
	"github.com/alibaba/polardbx-operator/api/v1/polardbx"

	domain_pxc "polardbx-dashboard-backend/pkg/api/domain/polardbxclusters"
)

func TestBackupScheduleEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Sample BackupSchedule for testing
	sampleBackupSchedule := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-backup-schedule",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupScheduleSpec{
			Schedule:       "0 2 * * *", // Daily at 2 AM
			Suspend:        false,
			MaxBackupCount: 7, // Keep 7 backups
			BackupSpec: polardbxv1.PolarDBXBackupSpec{
				Cluster: polardbxv1.PolarDBXClusterReference{
					Name: "test-cluster",
				},
				RetentionTime: metav1.Duration{Duration: 0}, // Use default
				CleanPolicy:   polardbx.CleanPolicyRetain,
				StorageProvider: polardbx.BackupStorageProvider{
					StorageName: polardbx.OSS,
					Sink:        "oss://test-bucket/backup/",
				},
				PreferredBackupRole: "follower",
			},
		},
		Status: polardbxv1.PolarDBXBackupScheduleStatus{
			LastBackup: "test-backup-001",
		},
	}

	// A client pre-populated with BackupSchedule
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sampleBackupSchedule).Build()

	router := gin.New()
	getErr := func(resp map[string]interface{}) string {
		switch v := resp["error"].(type) {
		case string:
			return v
		case map[string]interface{}:
			if msg, ok := v["message"].(string); ok {
				return msg
			}
		}
		return ""
	}
	router.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeClient)
		c.Next()
	})

	// Register BackupSchedule routes via domain handlers
	router.GET("/backup-schedules", domain_pxc.ListSchedules)
	router.POST("/backup-schedules", domain_pxc.CreateSchedule)
	router.GET("/backup-schedules/:namespace/:name", domain_pxc.GetSchedule)
	router.PUT("/backup-schedules/:namespace/:name", domain_pxc.UpdateSchedule)
	router.DELETE("/backup-schedules/:namespace/:name", domain_pxc.DeleteSchedule)

	// --- Test ListBackupSchedules ---
	t.Run("ListBackupSchedules", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/backup-schedules?namespace=default", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var schedules []polardbxv1.PolarDBXBackupSchedule
		err := json.Unmarshal(w.Body.Bytes(), &schedules)
		assert.NoError(t, err)
		assert.Len(t, schedules, 1)
		assert.Equal(t, "test-backup-schedule", schedules[0].Name)
		assert.Equal(t, "0 2 * * *", schedules[0].Spec.Schedule)
		assert.False(t, schedules[0].Spec.Suspend)
		assert.Equal(t, 7, schedules[0].Spec.MaxBackupCount)
	})

	// --- Test CreateBackupSchedule ---
	t.Run("CreateBackupSchedule", func(t *testing.T) {
		w := httptest.NewRecorder()
		newBackupSchedule := &polardbxv1.PolarDBXBackupSchedule{
			ObjectMeta: metav1.ObjectMeta{
				Name: "new-backup-schedule",
			},
			Spec: polardbxv1.PolarDBXBackupScheduleSpec{
				Schedule:       "0 1 * * *", // Daily at 1 AM
				Suspend:        false,
				MaxBackupCount: 5,
				BackupSpec: polardbxv1.PolarDBXBackupSpec{
					Cluster: polardbxv1.PolarDBXClusterReference{
						Name: "new-cluster",
					},
					CleanPolicy: polardbx.CleanPolicyDelete,
					StorageProvider: polardbx.BackupStorageProvider{
						StorageName: polardbx.MINIO,
						Sink:        "s3://test-bucket/backup/",
					},
				},
			},
		}
		body, _ := json.Marshal(newBackupSchedule)
		req, _ := http.NewRequest(http.MethodPost, "/backup-schedules?namespace=default", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusCreated, w.Code)

		// Verify it was created
		var createdSchedule polardbxv1.PolarDBXBackupSchedule
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "new-backup-schedule", Namespace: "default"}, &createdSchedule)
		assert.NoError(t, err)
		assert.Equal(t, "new-backup-schedule", createdSchedule.Name)
		assert.Equal(t, "0 1 * * *", createdSchedule.Spec.Schedule)
		assert.Equal(t, 5, createdSchedule.Spec.MaxBackupCount)
		assert.Equal(t, "new-cluster", createdSchedule.Spec.BackupSpec.Cluster.Name)
	})

	// --- Test GetBackupSchedule ---
	t.Run("GetBackupSchedule", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/backup-schedules/default/test-backup-schedule", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		var schedule polardbxv1.PolarDBXBackupSchedule
		err := json.Unmarshal(w.Body.Bytes(), &schedule)
		assert.NoError(t, err)
		assert.Equal(t, "test-backup-schedule", schedule.Name)
		assert.Equal(t, "0 2 * * *", schedule.Spec.Schedule)
		assert.Equal(t, "test-cluster", schedule.Spec.BackupSpec.Cluster.Name)
		assert.Equal(t, "test-backup-001", schedule.Status.LastBackup)
	})

	// --- Test UpdateBackupSchedule ---
	t.Run("UpdateBackupSchedule", func(t *testing.T) {
		w := httptest.NewRecorder()
		updatedSchedule := sampleBackupSchedule.DeepCopy()
		updatedSchedule.Spec.Schedule = "0 3 * * *" // Change to 3 AM
		updatedSchedule.Spec.Suspend = true         // Suspend the schedule
		updatedSchedule.Spec.MaxBackupCount = 10    // Keep 10 backups
		body, _ := json.Marshal(updatedSchedule)
		req, _ := http.NewRequest(http.MethodPut, "/backup-schedules/default/test-backup-schedule", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify it was updated
		var scheduleInClient polardbxv1.PolarDBXBackupSchedule
		err := fakeClient.Get(context.TODO(), client.ObjectKey{Name: "test-backup-schedule", Namespace: "default"}, &scheduleInClient)
		assert.NoError(t, err)
		assert.Equal(t, "0 3 * * *", scheduleInClient.Spec.Schedule)
		assert.True(t, scheduleInClient.Spec.Suspend)
		assert.Equal(t, 10, scheduleInClient.Spec.MaxBackupCount)
	})

	// --- Test DeleteBackupSchedule ---
	t.Run("DeleteBackupSchedule", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/backup-schedules/default/test-backup-schedule", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)

		// Verify deletion response
		var response map[string]string
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "backup schedule deleted", response["message"])
	})

	// --- Test CreateBackupSchedule with invalid JSON ---
	t.Run("CreateBackupScheduleInvalidJSON", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodPost, "/backup-schedules?namespace=default", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, strings.ToLower(getErr(response)), "invalid request format")
	})

	// --- Test GetBackupSchedule for non-existent schedule ---
	t.Run("GetNonExistentBackupSchedule", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/backup-schedules/default/non-existent-schedule", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, getErr(response), "not found")
	})

	// --- Test DeleteBackupSchedule for non-existent schedule ---
	t.Run("DeleteNonExistentBackupSchedule", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodDelete, "/backup-schedules/default/non-existent-schedule", nil)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Contains(t, getErr(response), "not found")
	})
}

func TestBackupScheduleBusinessLogic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	_ = polardbxv1.AddToScheme(scheme)

	// Test different storage providers
	t.Run("TestDifferentStorageProviders", func(t *testing.T) {
		testCases := []struct {
			name       string
			provider   polardbx.BackupStorage
			sink       string
			expectedOK bool
		}{
			{"OSS Storage", polardbx.OSS, "oss://bucket/path/", true},
			{"S3/MinIO Storage", polardbx.MINIO, "s3://bucket/path/", true},
			{"SFTP Storage", polardbx.SFTP, "sftp://server/path/", true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

				router := gin.New()
				router.Use(func(c *gin.Context) {
					c.Set("k8sClient", fakeClient)
					c.Next()
				})
				router.POST("/backup-schedules", domain_pxc.CreateSchedule)

				schedule := &polardbxv1.PolarDBXBackupSchedule{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-" + string(tc.provider),
					},
					Spec: polardbxv1.PolarDBXBackupScheduleSpec{
						Schedule: "0 1 * * *",
						BackupSpec: polardbxv1.PolarDBXBackupSpec{
							Cluster: polardbxv1.PolarDBXClusterReference{
								Name: "test-cluster",
							},
							StorageProvider: polardbx.BackupStorageProvider{
								StorageName: tc.provider,
								Sink:        tc.sink,
							},
						},
					},
				}

				body, _ := json.Marshal(schedule)
				w := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodPost, "/backup-schedules?namespace=default", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(w, req)

				if tc.expectedOK {
					assert.Equal(t, http.StatusCreated, w.Code)
				}
			})
		}
	})

	// Test different cron schedules
	t.Run("TestCronSchedules", func(t *testing.T) {
		testCases := []struct {
			name     string
			schedule string
			expected bool
		}{
			{"Daily at 2 AM", "0 2 * * *", true},
			{"Weekly on Sunday", "0 2 * * 0", true},
			{"Hourly", "0 * * * *", true},
			{"Every 6 hours", "0 */6 * * *", true},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

				router := gin.New()
				router.Use(func(c *gin.Context) {
					c.Set("k8sClient", fakeClient)
					c.Next()
				})
				router.POST("/backup-schedules", domain_pxc.CreateSchedule)

				schedule := &polardbxv1.PolarDBXBackupSchedule{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-schedule-" + tc.name,
					},
					Spec: polardbxv1.PolarDBXBackupScheduleSpec{
						Schedule: tc.schedule,
						BackupSpec: polardbxv1.PolarDBXBackupSpec{
							Cluster: polardbxv1.PolarDBXClusterReference{
								Name: "test-cluster",
							},
						},
					},
				}

				body, _ := json.Marshal(schedule)
				w := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodPost, "/backup-schedules?namespace=default", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				router.ServeHTTP(w, req)

				assert.Equal(t, http.StatusCreated, w.Code)
			})
		}
	})
}
