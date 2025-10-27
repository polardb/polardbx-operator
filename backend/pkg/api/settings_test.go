package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	api_settings "polardbx-ui-backend/pkg/api/settings"
)

func TestBackupDashboardSettings_DefaultsAndUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	corev1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	r := gin.Default()
	r.Use(func(c *gin.Context) { c.Set("k8sClient", fakeClient) })
	r.GET("/api/v1/settings/backup-dashboard", api_settings.Get)
	r.PUT("/api/v1/settings/backup-dashboard", api_settings.Update)

	// Defaults: expect empty map when no config present
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/settings/backup-dashboard", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	var s1 map[string]any
	json.Unmarshal(w.Body.Bytes(), &s1)
	// no predefined keys assumed

	// Update: write settings and read back
	payload := api_settings.BackupDashboardSettings{RPOThresholdSeconds: 7200, ThroughputLowerBoundMBps: 2.5, DiagnosisRetentionDays: 14}
	b, _ := json.Marshal(payload)
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodPut, "/api/v1/settings/backup-dashboard", bytes.NewReader(b))
	req2.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)

	// Get again -> returns raw ConfigMap Data map (string values)
	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest(http.MethodGet, "/api/v1/settings/backup-dashboard", nil)
	r.ServeHTTP(w3, req3)
	var s2 map[string]any
	json.Unmarshal(w3.Body.Bytes(), &s2)
	// Values are strings in ConfigMap Data; verify presence
	assert.Equal(t, "7200", s2["rpoThresholdSeconds"])
	assert.Equal(t, "2.5", s2["throughputLowerBoundMBps"])
	assert.Equal(t, "14", s2["diagnosisRetentionDays"])
}
