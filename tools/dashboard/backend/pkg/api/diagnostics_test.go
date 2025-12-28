package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	domain_diagnostics "polardbx-dashboard-backend/pkg/api/domain/platform/diagnostics/handler"
	diagservice "polardbx-dashboard-backend/pkg/api/domain/platform/diagnostics/service"
)

func TestDiagnostics_Placeholders(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = rbacv1.AddToScheme(scheme)
	fakeCli := crfake.NewClientBuilder().WithScheme(scheme).Build()

	r := gin.Default()
	r.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeCli)
	})
	r.POST("/api/v1/diagnostics/:namespace/:cluster/start", domain_diagnostics.Start)
	r.GET("/api/v1/diagnostics/:namespace/:id/status", domain_diagnostics.GetStatus)
	r.GET("/api/v1/diagnostics/reports", domain_diagnostics.ListReports)
	r.GET("/api/v1/diagnostics/:namespace/:id/download", domain_diagnostics.Download)
	r.GET("/api/v1/diagnostics/:namespace/:id/file", domain_diagnostics.GetFile)

	// Start
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/diagnostics/ns1/pxc-1/start", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusAccepted, w.Code)
	var started map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &started)
	jobID, _ := started["id"].(string)
	assert.NotEmpty(t, jobID)

	// Status
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/ns1/"+jobID+"/status", nil)
	r.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)
	var st map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &st)
	// Use real status values; we do not rely on placeholder literals.
	assert.Contains(t, []string{diagservice.DiagStatusPending, diagservice.DiagStatusRunning, diagservice.DiagStatusSucceeded, diagservice.DiagStatusFailed}, st["status"])

	// List
	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/reports?namespace=ns1", nil)
	r.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusOK, w3.Code)

	// Download
	w4 := httptest.NewRecorder()
	req4, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/ns1/"+jobID+"/download", nil)
	r.ServeHTTP(w4, req4)
	assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, w4.Code)

	// File (fallback in tests: job not succeeded -> conflict)
	w5 := httptest.NewRecorder()
	req5, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/ns1/"+jobID+"/file", nil)
	r.ServeHTTP(w5, req5)
	assert.Equal(t, http.StatusConflict, w5.Code)
}

func TestDiagnostics_GetFile_FallbackWhenNoAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	// Create a "new-style" clinic pod whose init container already completed successfully,
	// so GetFile proceeds to auth check (but we intentionally do NOT provide kubeconfig).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      diagservice.ClinicPodPrefix + "job-1",
			Namespace: "ns1",
			Labels: map[string]string{
				diagservice.ClinicLabelKey: "true",
				"polardbx/cluster":         "pxc-1",
			},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{Name: "collector"},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "collector",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
						},
					},
				},
			},
		},
	}
	fakeCli := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()

	r := gin.Default()
	r.Use(func(c *gin.Context) {
		c.Set("k8sClient", fakeCli)
		// Intentionally do NOT set normalizedKubeconfig.
	})
	r.GET("/api/v1/diagnostics/:namespace/:id/file", domain_diagnostics.GetFile)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/ns1/job-1/file", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
