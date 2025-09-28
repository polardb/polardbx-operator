package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	api_diagnostics "polardbx-ui-backend/pkg/api/diagnostics"
)

func TestDiagnostics_Placeholders(t *testing.T) {
	r := gin.Default()
	r.POST("/api/v1/diagnostics/:namespace/:cluster/start", api_diagnostics.Start)
	r.GET("/api/v1/diagnostics/:namespace/:id/status", api_diagnostics.GetStatus)
	r.GET("/api/v1/diagnostics/reports", api_diagnostics.ListReports)
	r.GET("/api/v1/diagnostics/:namespace/:id/download", api_diagnostics.Download)

	// Start
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(http.MethodPost, "/api/v1/diagnostics/ns1/pxc-1/start", nil)
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusAccepted, w.Code)

	// Status
	w2 := httptest.NewRecorder()
	req2, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/ns1/123/status", nil)
	r.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)
	var st map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &st)
	assert.Equal(t, "pending_implementation", st["status"])

	// List
	w3 := httptest.NewRecorder()
	req3, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/reports?namespace=ns1", nil)
	r.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusOK, w3.Code)

	// Download
	w4 := httptest.NewRecorder()
	req4, _ := http.NewRequest(http.MethodGet, "/api/v1/diagnostics/ns1/123/download", nil)
	r.ServeHTTP(w4, req4)
	assert.Equal(t, http.StatusOK, w4.Code)
}
