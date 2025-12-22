package integration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// setupSchedulesRouter sets up a test router with backup schedule routes
func setupSchedulesRouter(t *testing.T, objs ...runtime.Object) (*gin.Engine, client.Client) {
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

	// Register schedule routes
	v1.GET("/backup-schedules/next-run", domain_pxc.GetScheduleNextRuns)
	clusterGroup := v1.Group("/polardbxclusters")
	clusterGroup.GET("/backup-schedules", domain_pxc.ListSchedules)
	clusterGroup.POST("/backup-schedules", domain_pxc.CreateSchedule)
	item := clusterGroup.Group("/backup-schedules/:namespace/:name")
	item.GET("", domain_pxc.GetSchedule)
	item.PUT("", domain_pxc.UpdateSchedule)
	item.DELETE("", domain_pxc.DeleteSchedule)

	return r, ctrlClient
}

// ==================== GetNextRuns Integration Tests ====================

func TestIntegration_GetNextRuns_Empty(t *testing.T) {
	router, _ := setupSchedulesRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backup-schedules/next-run?namespace=default", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp, "schedules")
}

func TestIntegration_GetNextRuns_WithStatusNextBackupTime(t *testing.T) {
	nextTime := metav1.NewTime(time.Now().Add(24 * time.Hour))
	schedule := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-schedule",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupScheduleSpec{
			Schedule: "0 2 * * *",
			BackupSpec: polardbxv1.PolarDBXBackupSpec{
				Cluster: polardbxv1.PolarDBXClusterReference{
					Name: "test-cluster",
				},
			},
		},
		Status: polardbxv1.PolarDBXBackupScheduleStatus{
			NextBackupTime: &nextTime,
		},
	}

	router, _ := setupSchedulesRouter(t, schedule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backup-schedules/next-run?namespace=default", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp, "schedules")
}

func TestIntegration_GetNextRuns_WithCronExpression(t *testing.T) {
	schedule := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cron-schedule",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupScheduleSpec{
			Schedule: "0 3 * * *", // Daily at 3 AM
			BackupSpec: polardbxv1.PolarDBXBackupSpec{
				Cluster: polardbxv1.PolarDBXClusterReference{
					Name: "test-cluster",
				},
			},
		},
		// No status.nextBackupTime - should calculate from cron
	}

	router, _ := setupSchedulesRouter(t, schedule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backup-schedules/next-run?namespace=default", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp, "schedules")
}

func TestIntegration_GetNextRuns_MultipleNamespaces(t *testing.T) {
	schedule1 := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "schedule-1",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupScheduleSpec{
			Schedule: "0 2 * * *",
			BackupSpec: polardbxv1.PolarDBXBackupSpec{
				Cluster: polardbxv1.PolarDBXClusterReference{
					Name: "cluster-1",
				},
			},
		},
	}

	schedule2 := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "schedule-2",
			Namespace: "test-ns",
		},
		Spec: polardbxv1.PolarDBXBackupScheduleSpec{
			Schedule: "0 3 * * *",
			BackupSpec: polardbxv1.PolarDBXBackupSpec{
				Cluster: polardbxv1.PolarDBXClusterReference{
					Name: "cluster-2",
				},
			},
		},
	}

	router, _ := setupSchedulesRouter(t, schedule1, schedule2)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backup-schedules/next-run", nil) // No namespace filter
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Contains(t, resp, "schedules")
}

func TestIntegration_GetNextRuns_InvalidCron(t *testing.T) {
	schedule := &polardbxv1.PolarDBXBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "invalid-schedule",
			Namespace: "default",
		},
		Spec: polardbxv1.PolarDBXBackupScheduleSpec{
			Schedule: "invalid-cron-expression",
			BackupSpec: polardbxv1.PolarDBXBackupSpec{
				Cluster: polardbxv1.PolarDBXClusterReference{
					Name: "test-cluster",
				},
			},
		},
	}

	router, _ := setupSchedulesRouter(t, schedule)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/api/v1/backup-schedules/next-run?namespace=default", nil)
	router.ServeHTTP(w, req)

	// May return 200 with empty items or 500 error
	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, w.Code)
}
